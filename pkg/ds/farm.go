package ds

import (
	"square/up/util"
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/square/p2/pkg/ds/fields"
	ds_fields "github.com/square/p2/pkg/ds/fields"
	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/dsstore"
	"github.com/square/p2/pkg/labels"
	"github.com/square/p2/pkg/logging"
	"github.com/square/p2/pkg/scheduler"
	"github.com/square/p2/pkg/types"
	klabels "k8s.io/kubernetes/pkg/labels"
)

// Farm instatiates and deletes daemon sets as needed
type Farm struct {
	// constructor arguments
	kpStore    kp.Store
	dsStore    dsstore.Store
	scheduler  scheduler.Scheduler
	applicator labels.Applicator

	children map[fields.ID]*childDS
	childMu  sync.Mutex

	logger logging.Logger
}

type childDS struct {
	ds        DaemonSet
	quitCh    chan<- struct{}
	updatedCh chan<- *ds_fields.DaemonSet
	deletedCh chan<- *ds_fields.DaemonSet
	errCh     <-chan error
}

func NewFarm(
	kpStore kp.Store,
	dsStore dsstore.Store,
	applicator labels.Applicator,
	logger logging.Logger,
) *Farm {
	return &Farm{
		kpStore:    kpStore,
		dsStore:    dsStore,
		scheduler:  scheduler.NewApplicatorScheduler(applicator),
		applicator: applicator,
		logger:     logger,
		children:   make(map[fields.ID]*childDS),
	}
}

func (dsf *Farm) Start(quitCh <-chan struct{}) {
	err := dsf.cleanupDaemonSetPods()
	if err != nil {
		dsf.logger.Fatalf("Error unscheduling daemon set pods without existing daemon set: %v", err)
		return
	}

	dsf.mainLoop(quitCh)
}

// This function removes all pods with a DSIDLabel where the daemon set id does
// not exist in the store
func (dsf *Farm) cleanupDaemonSetPods() error {
	allDaemonSets, err := dsf.dsStore.List()
	dsIDMap := make(map[string]ds_fields.DaemonSet)
	for _, dsFields := range allDaemonSets {
		dsIDMap[dsFields.ID.String()] = dsFields
	}

	allPods, err := dsf.applicator.GetMatches(klabels.Everything(), labels.POD)
	if err != nil {
		return err
	}

	for _, podLabels := range allPods {
		// Only check if it is a pod scheduled by a daemon set
		if podLabels.Labels.Has(DSIDLabel) {
			dsID := podLabels.Labels.Get(DSIDLabel)

			// Check if the daemon set exists, if it doesn't unschedule the pod
			if _, ok := dsIDMap[dsID]; !ok {
				nodeName, podID, err := labels.NodeAndPodIDFromPodLabel(podLabels)
				if err != nil {
					return err
				}

				// TODO: Since this mirrors the unschedule function in daemon_set.go,
				// We should find a nice way to couple them together
				dsf.logger.NoFields().Infof("Unscheduling from %s with dangling daemon set uuid %s", nodeName, dsID)

				_, err = dsf.kpStore.DeletePod(kp.INTENT_TREE, nodeName, podID)
				if err != nil {
					return util.Errorf("Unable to delete pod from intent tree: %v", err)
				}

				id := labels.MakePodLabelKey(nodeName, podID)
				err = dsf.applicator.RemoveLabel(labels.POD, id, DSIDLabel)
				if err != nil {
					return util.Errorf("Error removing label: %v", err)
				}

			}
		}
	}

	return nil
}

func (dsf *Farm) mainLoop(quitCh <-chan struct{}) {
	subQuit := make(chan struct{})
	defer close(subQuit)
	dsWatch := dsf.dsStore.Watch(subQuit)

	var changes *dsstore.WatchedDaemonSets
	for {
		select {
		case <-quitCh:
			return
		case changes = <-dsWatch:
			if changes == nil {
				dsf.logger.Error("Unexpected nil value received from ds store watch in ds farm")
			}
		}

		if changes.Err != nil {
			dsf.logger.Infof("An error has occrred while watching daemon sets: %v", changes.Err)
		}

		if len(changes.Created) > 0 {
			dsf.logger.Infof("The following daemon sets have been created:")
			for _, dsFields := range changes.Created {
				dsf.logger.Infof("%v", *dsFields)
			}

			for _, dsFields := range changes.Created {
				// Only check for contention if it is not disabled
				if !dsFields.Disabled {
					// If the daemon set contends with another daemon set, disable it
					// if that fails, delete it, if that fails too this is a fatal error
					dsIDContended, contended, err := dsf.dsContends(dsFields)
					if err != nil {
						dsf.logger.Fatalf("Fatal error occured when trying to check for daemon set contention: %v", err)
						return
					}

					if contended {
						dsf.logger.Errorf("Created daemon set '%s' contends with %s", dsFields.ID, dsIDContended)
						newDS, err := dsf.disableDS(dsFields.ID)
						if err != nil {
							dsf.logger.Fatalf("Fatal error occured when trying to delete daemon set: %v", err)
							return
						}
						dsf.children[newDS.ID] = dsf.spawnDaemonSet(&newDS, quitCh)
					}
				}

				dsf.children[dsFields.ID] = dsf.spawnDaemonSet(dsFields, quitCh)
			}
		}

		if len(changes.Updated) > 0 {
			dsf.logger.Infof("The following daemon sets have been updated:")
			for _, dsFields := range changes.Updated {
				dsf.logger.Infof("%v", *dsFields)

				// If the daemon set contends with another daemon set, disable it
				// if that fails, delete it, if that fails too this is a fatal error
				if _, ok := dsf.children[dsFields.ID]; ok {
					// Only check for contention if it is not disabled
					if !dsFields.Disabled {
						dsIDContended, contended, err := dsf.dsContends(dsFields)
						if err != nil {
							dsf.logger.Fatalf("Fatal error occured when trying to check for daemon set contention: %v", err)
							return
						}

						if contended {
							dsf.logger.Errorf("Updated daemon set '%s' contends with %s", dsFields.ID, dsIDContended)
							newDS, err := dsf.disableDS(dsFields.ID)
							if err != nil {
								dsf.logger.Fatalf("Fatal error occured when trying to delete daemon set: %v", err)
								return
							}
							dsf.children[newDS.ID].updatedCh <- &newDS
						}
					}

					dsf.children[dsFields.ID].updatedCh <- dsFields
				}
			}
		}

		if len(changes.Deleted) > 0 {
			dsf.logger.Infof("The following daemon sets have been deleted:")
			for _, dsFields := range changes.Deleted {
				dsf.logger.Infof("%v", *dsFields)
				if child, ok := dsf.children[dsFields.ID]; ok {
					child.deletedCh <- dsFields
				}
			}
		}
	}
}

func (dsf *Farm) disableDS(dsID ds_fields.ID) (ds_fields.DaemonSet, error) {
	dsf.logger.Infof("Attempting to disable '%s' in store now", dsID)

	mutator := func(dsToUpdate ds_fields.DaemonSet) (ds_fields.DaemonSet, error) {
		dsToUpdate.Disabled = true
		return dsToUpdate, nil
	}
	newDS, err := dsf.dsStore.MutateDS(dsID, mutator)

	// Delete the daemon set because there was an error during mutation
	if err != nil {
		dsf.logger.Errorf("Error occured when trying to disable daemon set in store: %v, attempting to delete now", err)
		err = dsf.dsStore.Delete(dsID)
		// If you tried to delete it and there was an error, this is fatal
		if err != nil {
			return ds_fields.DaemonSet{}, err
		}
		dsf.logger.Infof("Deletion was successful for the daemon set: '%s' in store", dsID)
		return ds_fields.DaemonSet{}, nil
	}

	dsf.logger.Infof("Daemon set '%s' was successfully disabled in store", dsID)
	return newDS, nil
}

func (dsf *Farm) dsContends(dsFields *ds_fields.DaemonSet) (ds_fields.ID, bool, error) {
	eligibleNodes, err := dsf.scheduler.EligibleNodes(dsFields.Manifest, dsFields.NodeSelector)
	if err != nil {
		return "", false, util.Errorf("Error retrieving eligible nodes for daemon set: %v", err)
	}

	for _, nodeName := range eligibleNodes {
		id := labels.MakePodLabelKey(nodeName, dsFields.Manifest.ID())
		//err := ds.applicator.SetLabel(labels.POD, id, DSIDLabel, dsFields.ID().String())
		_, err := dsf.applicator.GetLabels(labels.POD, id)
		if err != nil {
			return "", false, util.Errorf("Error getting label: %v", err)
		}
	}

	for _, child := range dsf.children {
		// Check child if it is not disabled and that if has the same PodID
		if !child.ds.IsDisabled() && child.ds.PodID() == dsFields.PodID {
			scheduledNodes, err := child.ds.EligibleNodes()
			if err != nil {
				return "", false, util.Errorf("Error getting scheduled nodes: %v", err)
			}
			intersectedNodes := types.NewNodeSet(eligibleNodes...).Intersection(types.NewNodeSet(scheduledNodes...))
			if intersectedNodes.Len() > 0 {
				return child.ds.ID(), true, nil
			}
		}
	}

	return "", false, nil
}

func (dsf *Farm) spawnDaemonSet(dsFields *ds_fields.DaemonSet, quitCh <-chan struct{}) *childDS {
	dsLogger := dsf.logger.SubLogger(logrus.Fields{
		"ds":  dsFields.ID,
		"pod": dsFields.Manifest.ID(),
	})

	ds := New(
		*dsFields,
		dsf.dsStore,
		dsf.kpStore,
		dsf.applicator,
		dsLogger,
	)

	quitSpawnCh := make(chan struct{})
	updatedCh := make(chan *ds_fields.DaemonSet)
	deletedCh := make(chan *ds_fields.DaemonSet)

	desiresCh := ds.WatchDesires(quitSpawnCh, updatedCh, deletedCh)

	return &childDS{
		ds:        ds,
		quitCh:    quitSpawnCh,
		updatedCh: updatedCh,
		deletedCh: deletedCh,
		errCh:     desiresCh,
	}
}

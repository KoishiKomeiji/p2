package labels

import (
	"fmt"
	"sync"
	"time"

	"k8s.io/kubernetes/pkg/labels"
)

// This is a map of type -> id -> Set
// equivalently, of type -> id -> key -> value
type fakeApplicatorData map[Type]map[string]labels.Set

type fakeApplicator struct {
	// KV data that will be returned by queries
	data fakeApplicatorData
	// since entry() may mutate the map, every read can potentially trigger a
	// write. no point using rwmutex here
	mutex sync.Mutex
}

var _ Applicator = &fakeApplicator{}

func NewFakeApplicator() *fakeApplicator {
	return &fakeApplicator{data: make(fakeApplicatorData)}
}

func (app *fakeApplicator) entry(labelType Type, id string) map[string]string {
	if _, ok := app.data[labelType]; !ok {
		app.data[labelType] = make(map[string]labels.Set)
	}
	forType := app.data[labelType]
	if _, ok := forType[id]; !ok {
		forType[id] = make(labels.Set)
	}
	return forType[id]
}

func (app *fakeApplicator) SetLabel(labelType Type, id, name, value string) error {
	app.mutex.Lock()
	defer app.mutex.Unlock()
	entry := app.entry(labelType, id)
	entry[name] = value
	return nil
}

func (app *fakeApplicator) SetLabels(labelType Type, id string, labels map[string]string) error {
	app.mutex.Lock()
	defer app.mutex.Unlock()
	entry := app.entry(labelType, id)
	for k, v := range labels {
		entry[k] = v
	}
	return nil
}

func (app *fakeApplicator) RemoveAllLabels(labelType Type, id string) error {
	app.mutex.Lock()
	defer app.mutex.Unlock()
	delete(app.data[labelType], id)
	return nil
}

func (app *fakeApplicator) RemoveLabel(labelType Type, id, name string) error {
	app.mutex.Lock()
	defer app.mutex.Unlock()
	entry := app.entry(labelType, id)
	delete(entry, name)
	return nil
}

func (app *fakeApplicator) GetLabels(labelType Type, id string) (Labeled, error) {
	app.mutex.Lock()
	defer app.mutex.Unlock()
	entry := app.entry(labelType, id)
	return Labeled{
		ID:        id,
		LabelType: labelType,
		Labels:    copySet(entry),
	}, nil
}

func (app *fakeApplicator) GetMatches(selector labels.Selector, labelType Type) ([]Labeled, error) {
	app.mutex.Lock()
	defer app.mutex.Unlock()
	forType, ok := app.data[labelType]
	if !ok {
		return []Labeled{}, nil
	}

	results := []Labeled{}

	for id, set := range forType {
		if selector.Matches(set) {
			results = append(results, Labeled{
				ID:        id,
				LabelType: labelType,
				Labels:    copySet(set),
			})
		}
	}

	return results, nil
}

func (app *fakeApplicator) WatchMatches(selector labels.Selector, labelType Type, quitCh <-chan struct{}) chan []Labeled {
	ch := make(chan []Labeled)
	timer := time.NewTimer(time.Duration(0))
	go func() {
		for {
			select {
			case <-quitCh:
				return
			case <-timer.C:
			}
			// upper bound on request rate
			timer.Reset(100 * time.Millisecond)

			res, _ := app.GetMatches(selector, labelType)
			select {
			case <-quitCh:
				return
			case ch <- res:
			}

		}
	}()
	return ch
}

func (app *fakeApplicator) WatchMatchDiff(
	selector labels.Selector,
	labelType Type,
	quitCh <-chan struct{},
) <-chan *LabeledChanges {
	outCh := make(chan *LabeledChanges)

	go func() {
		defer close(outCh)

		var labelsList []Labeled
		inCh := app.WatchMatches(selector, labelType, quitCh)

		// Get a starting value to compare to
		for {
			select {
			case <-quitCh:
				return
			case labelsList = <-inCh:
				if labelsList == nil {
					fmt.Println("Unexpected nil value received from WatchMatches, recovered")
					continue
				}
			}
			break
		}

		oldLabels := make(map[string]Labeled)
		for _, labeled := range labelsList {
			oldLabels[labeled.ID] = labeled
		}

		for {
			var results []Labeled
			select {
			case <-quitCh:
				return
			case results = <-inCh:
				if results == nil {
					fmt.Println("Unexpected nil value received from WatchMatches, recovered")
					continue
				}
			}

			newLabels := make(map[string]Labeled)
			for _, labeled := range results {
				newLabels[labeled.ID] = labeled
			}

			outgoingChanges := &LabeledChanges{}
			for id, labeled := range newLabels {
				if _, ok := oldLabels[id]; !ok {
					// If it is not observed, then it was created
					outgoingChanges.Created = append(outgoingChanges.Created, Labeled{
						ID:        id,
						LabelType: labelType,
						Labels:    copySet(labeled.Labels),
					})
					oldLabels[id] = labeled

				} else if oldLabels[id].Labels.String() != labeled.Labels.String() {
					// Then it is in the map, if the values are not the same, it was an update
					outgoingChanges.Updated = append(outgoingChanges.Updated, Labeled{
						ID:        id,
						LabelType: labelType,
						Labels:    copySet(labeled.Labels),
					})
					oldLabels[id] = labeled
				}
			}
			// If it was not observed, then it was a delete
			for id, labeled := range oldLabels {
				if _, ok := newLabels[id]; !ok {
					outgoingChanges.Deleted = append(outgoingChanges.Deleted, Labeled{
						ID:        id,
						LabelType: labelType,
						Labels:    copySet(labeled.Labels),
					})
					delete(oldLabels, id)
				}
			}

			select {
			case <-quitCh:
				return
			case outCh <- outgoingChanges:
			}
		}
	}()

	return outCh
}

// avoid returning elements of the inner data map, otherwise concurrent callers
// may cause races when mutating them
func copySet(in labels.Set) labels.Set {
	ret := make(labels.Set, len(in))
	for k, v := range in {
		ret[k] = v
	}
	return ret
}

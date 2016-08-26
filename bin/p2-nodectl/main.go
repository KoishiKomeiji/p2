package main

import (
	"fmt"
	"log"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/square/p2/pkg/kp"
	"github.com/square/p2/pkg/kp/flags"
	"github.com/square/p2/pkg/labels"
	pc_fields "github.com/square/p2/pkg/pc/fields"
	klabels "k8s.io/kubernetes/pkg/labels"
)

const (
	CmdCreate = "create"
	CmdList   = "list"
	CmdDelete = "delete"
	CmdGet    = "get"
)

var (
	cmdCreate  = kingpin.Command(CmdCreate, "Create a node.")
	createName = cmdCreate.Arg("name", "The node name").Required().String()
	createAZ   = cmdCreate.Flag("az", "The availability zone of the pod cluster").Required().String()

	cmdList = kingpin.Command(CmdList, "List nodes.")

	cmdDelete  = kingpin.Command(CmdDelete, "Delete node.")
	deleteName = cmdDelete.Arg("name", "The node name").Required().String()

	cmdGet  = kingpin.Command(CmdGet, "Get node.")
	getName = cmdGet.Arg("name", "The node name").Required().String()
)

func main() {
	cmd, consulOpts := flags.ParseWithConsulOptions()
	client := kp.NewConsulClient(consulOpts)
	applicator := labels.NewConsulApplicator(client, 3)

	switch cmd {
	case CmdCreate:
		err := applicator.SetLabel(labels.NODE, *createName, pc_fields.AvailabilityZoneLabel, *createAZ)
		if err != nil {
			log.Fatalf("Error adding node: %v", err)
		}

	case CmdList:
		selector := klabels.Everything()
		nodes, err := applicator.GetMatches(selector, labels.NODE)
		if err != nil {
			log.Fatalf("Error getting all nodes: %v", err)
		}
		for _, val := range nodes {
			fmt.Printf("%v", val)
			fmt.Println()
		}

		nodes, err = applicator.GetMatches(selector, labels.POD)
		if err != nil {
			log.Fatalf("Error getting all nodes: %v", err)
		}
		for _, val := range nodes {
			fmt.Printf("%v", val)
			fmt.Println()
		}

	case CmdDelete:
		err := applicator.RemoveLabel(labels.NODE, *deleteName, pc_fields.AvailabilityZoneLabel)
		if err != nil {
			log.Fatalf("Error adding node: %v", err)
		}

	case CmdGet:
		node, err := applicator.GetLabels(labels.NODE, *getName)
		if err != nil {
			log.Fatalf("Error getting node %v", err)
		}
		fmt.Println(node)

		node, err = applicator.GetLabels(labels.POD, *getName)
		if err != nil {
			log.Fatalf("Error getting node %v", err)
		}
		fmt.Println(node)
		fmt.Println()

	default:
		log.Fatalf("Unrecognized command %v", cmd)
	}
}

func selectorFrom(az pc_fields.AvailabilityZone) klabels.Selector {
	return klabels.Everything().
		Add(pc_fields.AvailabilityZoneLabel, klabels.InOperator, []string{az.String()})
}

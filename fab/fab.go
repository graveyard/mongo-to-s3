package fab

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"log"
	"os/exec"
	"strings"
)

const (
	serverUrlFormat = "mongodb-%s-dev.ops.clever.com:27017/%v"
)

type Instance struct {
	ID         string
	IP         string
	SnapshotID string
	URL        string
}

func CreateSISDBFromLatestSnapshot(name string) (Instance, error) {
	options := fmt.Sprintf("mongodb.create:%v,kind=small,env=dev,snapshot_search=mongodb-clever,latest_snapshot=True,skip_confirm=True", name)
	cmd := exec.Command("fab", options)

	var out, errout bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errout

	err := cmd.Run()
	if err != nil {
		return Instance{}, err
	}

	// Create a scanner to read for the mongo instance information
	var snapshotID string
	scanner := bufio.NewScanner(bytes.NewReader(out.Bytes()))
	for scanner.Scan() {
		text := scanner.Text()
		fmt.Println(text)
		line := strings.Split(text, " ")
		if len(line) == 5 && strings.Contains(text, "Restoring image from snapshot_id") {
			snapshotID = line[4]
			snapshotID = strings.Replace(snapshotID, ".", "", 1)
			snapshotID = strings.Replace(snapshotID, "'", "", 2)
			log.Println("snapshotID: ", snapshotID)
		}
		if len(line) == 4 && strings.Contains(text, "mongo_instance:") {
			return Instance{
				ID:         line[1],
				IP:         line[3],
				SnapshotID: snapshotID,
				URL:        fmt.Sprintf(serverUrlFormat, name, "clever"),
			}, nil
		}
	}

	return Instance{}, errors.New("unable to find output")
}

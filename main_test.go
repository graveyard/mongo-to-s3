package main

import (
	"encoding/json"
	"io/ioutil"
	"testing"

	"github.com/Clever/mongo-to-s3/config"
	"github.com/stretchr/testify/assert"
)

func TestTableRetrieval(t *testing.T) {
	table1 := config.Table{Destination: "schools_dest", Source: "schools_source"}
	table2 := config.Table{Destination: "teachers_dest", Source: "teachers_source"}
	table3 := config.Table{Destination: "students_dest", Source: "students_source"}
	testConfig := config.Config{"schools": table1, "teachers": table2, "students": table3}

	// Test regular select source table
	table, err := getTableFromConf("students_source", testConfig)
	assert.NoError(t, err)
	assert.Equal(t, table.Destination, "students_dest")

	// Return error if no match
	table, err = getTableFromConf("foo", testConfig)
	assert.Error(t, err)

	// Return error if trying to get multiple collections
	table, err = getTableFromConf("students_source,schools_source", testConfig)
	assert.Error(t, err)

	// Return error if no collection specified
	table, err = getTableFromConf("", testConfig)
	assert.Error(t, err)
}

func TestCreateManifest(t *testing.T) {
	reader, err := createManifest("bucket", []string{"foo", "bar"})
	assert.NoError(t, err)
	expectedManifest := &Manifest{
		EntryArray{
			map[string]interface{}{"url": "s3://bucket/foo", "mandatory": true},
			map[string]interface{}{"url": "s3://bucket/bar", "mandatory": true},
		},
	}

	bytes, err := ioutil.ReadAll(reader)
	assert.NoError(t, err)
	manifest := &Manifest{}
	err = json.Unmarshal(bytes, manifest)
	assert.NoError(t, err)
	// check that the manifest entries match
	assert.Equal(t, expectedManifest.Entries, manifest.Entries)
}

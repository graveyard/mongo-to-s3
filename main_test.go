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
	testConfig1 := config.Config{"schools": table1, "teachers": table2, "students": table3}

	// Test regular select source tables, in order specified
	tables, err := getTablesFromConf("students_source,schools_source", testConfig1)
	assert.NoError(t, err)
	assert.Equal(t, len(tables), 2)
	assert.Equal(t, tables[0].Destination, "students_dest")
	assert.Equal(t, tables[1].Destination, "schools_dest")

	// Return error if none match
	tables, err = getTablesFromConf("foo", testConfig1)
	assert.Error(t, err)

	// Get all tables if none specified
	testConfig2 := config.Config{"America": config.Table{Destination: "Hawaii"}}
	tables, err = getTablesFromConf("", testConfig2)
	assert.NoError(t, err)
	assert.Equal(t, len(tables), 1)
	assert.Equal(t, tables[0].Destination, "Hawaii")
}

func TestCreateManifest(t *testing.T) {
	reader, err := createManifest([]string{"foo", "bar"})
	assert.NoError(t, err)
	expectedManifest := &Manifest{
		EntryArray{
			map[string]interface{}{"url": "foo", "mandatory": true},
			map[string]interface{}{"url": "bar", "mandatory": true},
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

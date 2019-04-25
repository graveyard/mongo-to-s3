package main

import (
	"encoding/json"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
)

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

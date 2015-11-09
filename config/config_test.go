package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/Clever/optimus.v3"
)

const (
	valid = `
  table1:
    dest: table1_dest
    source: table1_source
    columns:
    -
      dest: id
      source: _id
      type: text
    -
      dest: district_id
      source: district
      type: text
    -
      dest: type
      source: data.type
      type: text
`

	invalid1 = `clever:
  table1:
    dest: table1_dest
    source: table1_source
    columns:
      id:
        source: _id
	type: text
`
)

func TestValidYAML(t *testing.T) {
	config, err := ParseYAML([]byte(valid))
	assert.NoError(t, err)

	table := config["table1"]
	assert.Equal(t, "table1_dest", table.Destination)
	assert.Equal(t, "table1_source", table.Source)

	fields := []Field{
		{
			Destination: "id",
			Source:      "_id",
		}, {
			Destination: "district_id",
			Source:      "district",
		}, {
			Destination: "type",
			Source:      "data.type",
		},
	}

	for idx, field := range table.Fields {
		assert.Equal(t, fields[idx].Destination, field.Destination)
		assert.Equal(t, fields[idx].Source, field.Source)
	}
}

func TestInvalidYAML(t *testing.T) {
	_, err := ParseYAML([]byte(invalid1))
	assert.Error(t, err)
}

func TestMongoSelector(t *testing.T) {
	table := Table{
		Fields: []Field{
			{
				Source: "test1",
			}, {
				Destination: "test",
				Source:      "test2",
			}, {
				Destination: "test3",
				Source:      "test3",
			},
		},
	}

	selector := table.MongoSelector()

	// Check that every source is properly selected
	assert.Equal(t, 1, selector["test1"])
	assert.Equal(t, 1, selector["test2"])
	assert.Equal(t, 1, selector["test3"])

	// Check that destinations are not being selected
	_, ok := selector["test"]
	assert.False(t, ok)
}

func TestFieldMap(t *testing.T) {
	table := Table{
		Fields: []Field{
			{
				Destination: "test1",
				Source:      "test1",
			}, {
				Destination: "test_2",
				Source:      "test2",
			}, {
				Destination: "test3",
				Source:      "test2",
			}, {
				Destination: "foo",
				Source:      "test.foo",
			},
		},
	}

	mapping := table.FieldMap()

	// Assert fields are mapped correctly source -> destination
	assert.Equal(t, []string{"test1"}, mapping["test1"])
	assert.Equal(t, []string{"test_2", "test3"}, mapping["test2"])
	assert.Equal(t, []string{"foo"}, mapping["test.foo"])

	// Check for destinations being mapped
	_, ok := mapping["test3"]
	assert.False(t, ok)
}

func TestFlatten(t *testing.T) {
	test := []optimus.Row{
		{"foo": map[string]interface{}{"bar": map[string]interface{}{"boom": 1}, "baz": 2}},
		{"foo": optimus.Row{"bar": map[string]interface{}{"boom": "1"}, "baz": "2"}},
		{"abc": 123},
		{"def": []string{"1", "2"}},
		{"auth_requests_test": []interface{}{
			map[string]interface{}{"type": "sis"}}},
		{"auth_requests_test_long": []interface{}{
			map[string]interface{}{"a": "b", "c": "d"},
			map[string]interface{}{"e": "f"}}},
	}

	expected := []optimus.Row{
		{"foo.bar.boom": 1, "foo.baz": 2},
		{"foo.bar.boom": "1", "foo.baz": "2"},
		{"abc": 123},
		{"def": []string{"1", "2"}},
		{"auth_requests_test": `[{"type":"sis"}]`},
		{"auth_requests_test_long": `[{"a":"b","c":"d"},{"e":"f"}]`},
	}

	f := Flattener()
	for i, val := range test {
		valueRet, err := f(val)
		assert.NoError(t, err)
		assert.Equal(t, expected[i], valueRet)
	}
}

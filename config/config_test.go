package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/Clever/optimus.v3"
	"gopkg.in/Clever/optimus.v3/sources/slice"
	"gopkg.in/Clever/optimus.v3/tests"
	"gopkg.in/Clever/optimus.v3/transformer"
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

func TestFieldMap(t *testing.T) {
	table := Table{
		Fields: []Field{
			{
				Destination: "name",
				Source:      "name",
			}, {
				Destination: "test_2",
				Source:      "test2",
			}, {
				Destination: "test3",
				Source:      "test2",
			}, {
				Destination: "foo",
				Source:      "test.foo",
			}, {
				Destination: "name_first",
				Source:      "name.first",
			},
		},
	}

	mapping := table.FieldMap()

	// Assert fields are mapped correctly source -> destination
	assert.Equal(t, []string{"name"}, mapping["name"])
	assert.Equal(t, []string{"test_2", "test3"}, mapping["test2"])
	assert.Equal(t, []string{"foo"}, mapping["test.foo"])
	assert.Equal(t, []string{"name_first"}, mapping["name.first"])

	// Check for destinations being mapped
	_, ok := mapping["test3"]
	assert.False(t, ok)
}

func TestOptimusFieldMap(t *testing.T) {
	table := Table{
		Fields: []Field{
			{
				Destination: "name",
				Source:      "name",
			}, {
				Destination: "name_first",
				Source:      "name.first",
			},
		},
	}
	mapping := table.FieldMap()

	inputRows := []optimus.Row{
		{"name": "foo"},
		{"name.first": "bar"},
		{"do_not_want": "nope"}, // ensure fields we don't want are not mapped
	}
	expectedRows := []optimus.Row{
		{"name": "foo"},
		{"name_first": "bar"},
		{},
	}

	resTable := transformer.New(slice.New(inputRows)).Fieldmap(mapping).Table()
	rows := tests.GetRows(resTable)

	for i := range rows {
		assert.Equal(t, expectedRows[i], rows[i])
	}
}

func TestFlatten(t *testing.T) {
	test := []optimus.Row{
		{"foo": map[string]interface{}{"bar": map[string]interface{}{"boom": 1}, "baz": 2}},
		{"foo": optimus.Row{"bar": map[string]interface{}{"boom": "1"}, "baz": "2"}},
		{"abc": 123},
		{"name": "foo"},
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
		{"name": "foo"},
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

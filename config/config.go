package config

import (
	"gopkg.in/Clever/optimus.v3"
	"gopkg.in/yaml.v2"
)

type Config map[string]Table

type Table struct {
	Destination string  `yaml:"dest"`
	Source      string  `yaml:"source"`
	Fields      []Field `yaml:"columns"`
	Meta        Meta    `yaml:"meta"`
}

type Field struct {
	Destination string `yaml:"dest"`
	Source      string `yaml:"source"`
}

type Meta struct {
	Database       string `yaml:"database"`
	DataDateColumn string `yaml:"datadatecolumn"`
}

// ParseYAML marshalls data into a Config
func ParseYAML(data []byte) (Config, error) {
	config := Config{}
	err := yaml.Unmarshal(data, &config)
	return config, err
}

// MongoSelector returns a map that can be used in a mongo Select query
// to limit the fields returned from Mongo
func (t Table) MongoSelector() map[string]interface{} {
	selector := make(map[string]interface{})

	for _, field := range t.Fields {
		selector[field.Source] = 1
	}

	return selector
}

// FieldMap returns a mapping of all fields between source and destination
func (t Table) FieldMap() map[string][]string {
	mappings := make(map[string][]string)

	for _, field := range t.Fields {
		if field.Destination != "" {
			list := mappings[field.Source]
			mappings[field.Source] = append(list, field.Destination)
		}
	}

	return mappings
}

// GetPopulateDateFn returns a function which creates and populates the data date column
// we do this so that we have a good idea of when the data was created downstream
func (t Table) GetPopulateDateFn(dataDateColumn, timestamp string) func(optimus.Row) (optimus.Row, error) {
	return func(r optimus.Row) (optimus.Row, error) {
		r[dataDateColumn] = timestamp
		return r, nil
	}
}

// Flattener returns a function which flattens nested optimus rows into flat rows
// with dot-separated keys
func Flattener() func(optimus.Row) (optimus.Row, error) {
	return func(r optimus.Row) (optimus.Row, error) {
		outRow := map[string]interface{}{}
		flatten(rowToMap(r), "", &outRow)
		return optimus.Row(outRow), nil
	}
}

func rowToMap(r optimus.Row) map[string]interface{} {
	m := map[string]interface{}{}
	for k, v := range r {
		m[k] = v
	}
	return m
}

// flattens a nested json struct
// to start, pass "" as a lkey
func flatten(inputJSON map[string]interface{}, lkey string, flattened *map[string]interface{}) {
	for rkey, value := range inputJSON {
		key := lkey + rkey
		switch v := value.(type) {
		case map[string]interface{}:
			flatten(v, key+".", flattened)
		case optimus.Row:
			flatten(rowToMap(v), key+".", flattened)
		default:
			(*flattened)[key] = v
		}
	}
}

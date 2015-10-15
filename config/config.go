package config

import (
	"gopkg.in/yaml.v2"
)

type Config map[string]Table

type Table struct {
	Destination string  `yaml:"dest"`
	Source      string  `yaml:"source"`
	Fields      []Field `yaml:"columns"`
}

type Field struct {
	Destination string `yaml:"dest"`
	Source      string `yaml:"source"`
}

type Meta struct {
	Database string `yaml:"database"`
}

func ParseYAML(data []byte) (Config, error) {
	config := Config{}
	err := yaml.Unmarshal(data, &config)
	return config, err
}

func (t Table) MongoSelector() map[string]interface{} {
	selector := make(map[string]interface{})

	for _, field := range t.Fields {
		selector[field.Source] = 1
	}

	return selector
}

func (t Table) FieldMap() map[string][]string {
	mappings := make(map[string][]string)

	for _, field := range t.Fields {
		if field.Destination == "" {
			continue
		}
		list := mappings[field.Source]
		mappings[field.Source] = append(list, field.Destination)
	}

	return mappings
}

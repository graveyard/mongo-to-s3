package main

import (
	"testing"

	"github.com/Clever/mongo-to-s3/config"
	"github.com/stretchr/testify/assert"
)

func TestTableOrdering(t *testing.T) {
	table1 := config.Table{Destination: "schools"}
	table2 := config.Table{Destination: "students"}
	testConfig1 := config.Config{"schools": table1, "students": table2}
	for i := 0; i < 10; i++ {
		tables := orderTables(testConfig1)
		assert.Equal(t, tables[1].Destination, "students")
	}

	// Don't complain if "students" doesn't exist
	table3 := config.Table{Destination: "Hawaii"}
	table4 := config.Table{Destination: "Iceland"}
	testConfig2 := config.Config{"America": table3, "Europe": table4}
	orderTables(testConfig2)
}

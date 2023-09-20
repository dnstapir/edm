package main

import (
	"strconv"

	"github.com/apache/arrow/go/v13/arrow"
)

// Based on https://github.com/dnstapir/datasets/blob/main/dnstap2clickhouse.schema
func createLabelFields() []arrow.Field {
	arrowFields := []arrow.Field{}
	for i := 0; i < 10; i++ {
		arrowFields = append(
			arrowFields,
			arrow.Field{
				Name:     "label" + strconv.Itoa(i),
				Type:     arrow.BinaryTypes.String,
				Nullable: true,
			},
		)
	}

	return arrowFields
}

func dnsSessionRowArrowSchema() *arrow.Schema {
	arrowFields := []arrow.Field{}

	// FQDN as key
	arrowFields = append(arrowFields, createLabelFields()...)

	// Timestamps
	arrowFields = append(arrowFields, arrow.Field{Name: "query_time", Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: true})
	arrowFields = append(arrowFields, arrow.Field{Name: "response_time", Type: arrow.FixedWidthTypes.Timestamp_ns, Nullable: true})

	return arrow.NewSchema(
		arrowFields,
		nil,
	)
}

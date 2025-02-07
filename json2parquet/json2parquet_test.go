package json2parquet_test

import (
	"os"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/loicalleyne/bodkin/json2parquet"
	"github.com/stretchr/testify/assert"
)

var testFilePath = "/Users/thomasmcgeehan/bodkin/bodkin/data"

var testFile = testFilePath + "/internal_2732.json"

func TestFromReader(t *testing.T) {
	filepath := testFile

	f, err := os.Open(filepath)
	if err != nil {
		t.Fatalf("Failed to open test file: %v", err)
	}
	defer f.Close()

	schema, count, err := json2parquet.FromReader(f)
	if err != nil {
		t.Fatalf("FromReader failed: %v", err)
	}

	// Verify we got a valid schema
	assert.NotNil(t, schema, "Schema should not be nil")
	assert.Greater(t, count, 0, "Record count should be greater than 0")

	// Test converting to parquet
	outputPath := testFilePath + "/output.parquet"
	recordCount, err := json2parquet.RecordsFromFile(filepath, outputPath, schema, nil)
	if err != nil {
		t.Fatalf("RecordsFromFile failed: %v", err)
	}

	// We expect the counts to be equal
	assert.Equal(t, count, recordCount, "Record counts should match")

	// Cleanup
	os.Remove(outputPath)
}

func TestSchemaFromFile(t *testing.T) {
	filepath := testFile

	schema, count, err := json2parquet.SchemaFromFile(filepath)
	if err != nil {
		t.Fatalf("SchemaFromFile failed: %v", err)
	}

	// Verify schema properties
	assert.NotNil(t, schema, "Schema should not be nil")
	assert.Greater(t, count, 0, "Record count should be greater than 0")
	assert.Greater(t, len(schema.Fields()), 0, "Schema should have fields")

	// Test schema field types
	validateSchemaFields(t, schema)
}

func validateSchemaFields(t *testing.T, schema *arrow.Schema) {
	for _, field := range schema.Fields() {
		assert.NotEmpty(t, field.Name, "Field name should not be empty")
		assert.NotNil(t, field.Type, "Field type should not be nil")
	}
}

func TestFromReaderWithInvalidInput(t *testing.T) {
	// Test with empty reader
	emptyFile := testFilePath + "/empty.json"
	f, err := os.Create(emptyFile)
	if err != nil {
		t.Fatalf("Failed to create empty test file: %v", err)
	}
	defer func() {
		f.Close()
		os.Remove(emptyFile)
	}()

	schema, count, err := json2parquet.FromReader(f)
	assert.Error(t, err, "Should error on empty input")
	assert.Equal(t, 0, count, "Count should be 0 for empty input")
	assert.Nil(t, schema, "Schema should be nil for empty input")
}

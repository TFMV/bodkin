package json2parquet

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/loicalleyne/bodkin"
	"github.com/loicalleyne/bodkin/pq"
)

func FromReader(r io.Reader, opts ...bodkin.Option) (*arrow.Schema, int, error) {
	var err error
	s := bufio.NewScanner(r)
	u := bodkin.NewBodkin(opts...)
	for s.Scan() {
		err = u.Unify(s.Bytes())
		if err != nil {
			return nil, 0, err
		}
		if u.Count() > u.MaxCount() {
			break
		}
	}
	if err = s.Err(); err != nil {
		return nil, 0, err
	}

	schema, err := u.Schema()
	if err != nil {
		return nil, 0, err
	}
	return schema, u.Count(), nil
}

func SchemaFromFile(inputFile string, opts ...bodkin.Option) (*arrow.Schema, int, error) {
	f, err := os.Open(inputFile)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()

	r := bufio.NewReaderSize(f, 1024*32)
	return FromReader(r, opts...)
}

func RecordsFromFile(inputFile, outputFile string, schema *arrow.Schema, munger func(io.Reader, io.Writer) error, opts ...parquet.WriterProperty) (int, error) {
	n := 0
	f, err := os.Open(inputFile)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	var prp *parquet.WriterProperties = pq.DefaultWrtp
	if len(opts) != 0 {
		prp = parquet.NewWriterProperties(opts...)
	}
	pw, _, err := pq.NewParquetWriter(schema, prp, outputFile)
	if err != nil {
		return 0, err
	}
	defer pw.Close()

	r := bufio.NewReaderSize(f, 1024*1024*128)
	rdr := array.NewJSONReader(r, schema, array.WithChunk(1024))
	defer rdr.Release()

	for rdr.Next() {
		rec := rdr.Record()
		if err := pw.WriteRecord(rec); err != nil {
			return n, fmt.Errorf("failed to write parquet record: %v", err)
		}
		n += int(rec.NumRows())
	}

	if err := rdr.Err(); err != nil {
		return n, err
	}

	return n, pw.Close()
}

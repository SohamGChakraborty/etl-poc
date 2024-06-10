package etl

import (
	"encoding/csv"
	"errors"
	"io"
	"os"
)

type Reader interface {
	Next() ([][]string, error)
	Close() error
}

// reader reads data from a CSV file in batches.
type reader struct {
	file       *os.File
	csvReader  *csv.Reader
	batchSize  int
	validators []Validator
}

func NewCsvReader(filename string, batchSize int, validators []Validator) (Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	csvReader := csv.NewReader(file)

	return &reader{file: file, csvReader: csvReader, batchSize: batchSize, validators: validators}, nil
}

func (r *reader) Next() ([][]string, error) {
	var batch [][]string
	for i := 0; i < r.batchSize; i++ {
		record, err := r.csvReader.Read()
		if err != nil {
			if errors.Is(err, os.ErrClosed) || err == io.EOF {
				break
			}
			return nil, err
		}

		// IsValid the record
		for _, validator := range r.validators {
			if err := validator.IsValid(record); err != nil {
				return nil, err
			}
		}

		batch = append(batch, record)
	}
	return batch, nil
}

func (r *reader) Close() error {
	return r.file.Close()
}

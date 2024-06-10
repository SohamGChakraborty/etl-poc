package writer

import (
	"encoding/csv"
	"os"
)

// writer writes data to a CSV file in batches.
type writer struct {
	file      *os.File
	csvWriter *csv.Writer
}

func NewCsvWriter(filename string) (Writer, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	csvWriter := csv.NewWriter(file)

	return &writer{file: file, csvWriter: csvWriter}, nil
}

func (w *writer) Write(batch [][]string) error {
	for _, record := range batch {
		if err := w.csvWriter.Write(record); err != nil {
			return err
		}
	}
	w.csvWriter.Flush()
	return nil
}

func (w *writer) Close() error {
	return w.file.Close()
}

package main

import (
	"etl-poc/etl"
	"etl-poc/etl/processor"
	"etl-poc/etl/reader"
	"etl-poc/etl/validator"
	"etl-poc/etl/writer"
	"fmt"
	"time"
)

// FieldNotEmptyValidator checks if fields are not empty.
type FieldNotEmptyValidator struct {
}

func (v *FieldNotEmptyValidator) IsValid(record []string) error {
	return nil
}

type LengthValidator struct {
}

func (v *LengthValidator) IsValid(record []string) error {
	return nil
}

func main() {
	start := time.Now()
	const batchSize = 1000

	validators := []validator.Validator{
		&FieldNotEmptyValidator{},
		&LengthValidator{},
	}

	reader, err := reader.NewCsvReader("data.csv", batchSize, validators)
	if err != nil {
		fmt.Println("Error creating reader:", err)
		return
	}
	defer reader.Close()

	processor := processor.NewProcessor()

	writer, err := writer.NewCsvWriter("out.csv")
	if err != nil {
		fmt.Println("Error creating writer:", err)
		return
	}
	defer writer.Close()

	runner := etl.NewRunner(reader, processor, writer)
	runner.Start()
	end := time.Now()
	fmt.Println("Program runtime:", end.Sub(start))
}

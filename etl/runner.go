package etl

import (
	processor2 "etl-poc/etl/processor"
	reader2 "etl-poc/etl/reader"
	writer2 "etl-poc/etl/writer"
	"fmt"
	"sync"
)

type Runner struct {
	reader    reader2.Reader
	processor processor2.Processor
	writer    writer2.Writer
}

func NewRunner(reader reader2.Reader, processor processor2.Processor, writer writer2.Writer) *Runner {
	return &Runner{reader: reader, processor: processor, writer: writer}
}

func (r *Runner) Start() {
	var wg sync.WaitGroup
	readChan := make(chan [][]string)
	processChan := make(chan [][]string)
	doneChan := make(chan struct{})

	// Reader Goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(readChan)
		for {
			batch, err := r.reader.Next()
			if err != nil {
				fmt.Println("Error reading batch:", err)
				return
			}
			if len(batch) == 0 {
				return
			}
			readChan <- batch
		}
	}()

	// Processor Goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(processChan)
		for batch := range readChan {
			processedBatch, err := r.processor.Process(batch)
			if err != nil {
				fmt.Println("Error processing batch:", err)
				return
			}
			processChan <- processedBatch
		}
	}()

	// Writer Goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		for processedBatch := range processChan {
			if err := r.writer.Write(processedBatch); err != nil {
				fmt.Println("Error writing batch:", err)
				return
			}
		}
		doneChan <- struct{}{}
	}()

	go func() {
		wg.Wait()
		close(doneChan)
	}()

	<-doneChan
	fmt.Println("ETL process completed.")
}

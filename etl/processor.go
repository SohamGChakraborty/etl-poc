package etl

// processor processes data in batches.
type processor struct{}

type Processor interface {
	Process(batch [][]string) ([][]string, error)
}

func NewProcessor() Processor {
	return &processor{}
}

func (p *processor) Process(batch [][]string) ([][]string, error) {
	var processedBatch [][]string
	for _, record := range batch {
		var processedRecord []string
		for _, field := range record {
			processedRecord = append(processedRecord, field+"_transformed")
		}
		processedBatch = append(processedBatch, processedRecord)
	}
	return processedBatch, nil
}

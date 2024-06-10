package processor

type Processor interface {
	Process(batch [][]string) ([][]string, error)
}

package writer

type Writer interface {
	Write(batch [][]string) error
	Close() error
}

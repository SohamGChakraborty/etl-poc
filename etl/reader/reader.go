package reader

type Reader interface {
	Next() ([][]string, error)
	Close() error
}

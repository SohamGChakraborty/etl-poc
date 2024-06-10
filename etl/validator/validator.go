package validator

// Validator interface for validating records.
type Validator interface {
	IsValid(record []string) error
}

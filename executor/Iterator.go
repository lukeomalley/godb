package executor

// Iterator represents the interface of a iterator qurey plan node
type Iterator interface {
	Init()
	Close()
	Next() bool
	Execute() Tuple
}

package executor

// Executor handles the execution of a query plan against an slice of tuples
type Executor struct {
	tuples    []Tuple
	iterators []Iterator
}

// NewExecutor constructs a new executor
func NewExecutor(iterators []Iterator, tuples []Tuple) *Executor {
	return &Executor{tuples: tuples, iterators: iterators}
}

// Run executes the query plan and returns the resulting tuples
func (e *Executor) Run() []Tuple {
	result := []Tuple{}

	e.iterators[0].Init()

	for e.iterators[0].Next() {
		result = append(result, e.iterators[0].Execute())
	}

	return result
}

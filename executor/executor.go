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

func newTuple(inputs ...interface{}) Tuple {
	if len(inputs)%2 != 0 {
		panic("number of inputs to newTuple must be even")
	}

	tuple := Tuple{Values: make([]Value, 0, len(inputs)/2)}

	for i := 0; i < len(inputs); i += 2 {
		tuple.Values = append(tuple.Values, Value{
			Key:   inputs[i].(string),
			Value: inputs[i+1].(string),
		})
	}

	return tuple
}

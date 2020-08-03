package executor

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

	for _, iterator := range e.iterators {
		iterator
	}

	return result
}

// NewQueryPlan constructs a query plan
func NewQueryPlan(input []Operator) *QueryPlan {
	qp := &QueryPlan{operators: input}
	return qp
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

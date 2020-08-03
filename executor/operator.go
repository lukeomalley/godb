package executor

import (
	"fmt"
)

// Tuple represents a single tuple from a database table.
type Tuple struct {
	Values []Value
}

// Value represents a row, column value in a single tuple.
type Value struct {
	Key   string
	Value string
}

// OperatorType is an enum that represents the types of operators.
type OperatorType int

// Enum for the types of operators
const (
	SCAN OperatorType = iota
	FILTER
	PROJECTION
)

// Operator represents a single operator in a query plan.
type Operator struct {
	name       OperatorType
	parameters []string
}

// OperatorParser takes a slice of operators and returns a slice of iterators
type OperatorParser struct {
	tuples    []Tuple
	operators []Operator
	currIdx   int
	iterators []Iterator // Output
}

// NewOperatorParser constructs a new operator parser
func NewOperatorParser(operators []Operator, tuples []Tuple) *OperatorParser {
	return &OperatorParser{operators: operators, tuples: tuples, currIdx: 0, iterators: []Iterator{}}
}

// ParseQueryPlan parses a query plan of operators and returns
// a tree or iterator nodes
func (op *OperatorParser) ParseQueryPlan() []Iterator {
	for op.currIdx <= len(op.operators)-1 {
		op.iterators = append(op.iterators, op.parseOperator(op.operators[op.currIdx]))
		op.currIdx++
	}

	return op.iterators
}

func (op *OperatorParser) parseOperator(operator Operator) Iterator {
	switch operator.name {
	case SCAN:
		return NewScanIterator(op.tuples)
	case FILTER:
		// Parse the next operator to be passed into the FilterIterator
		nextIterator := op.parseOperator(op.operators[op.currIdx+1])
		op.currIdx++

		exp := parseFilterParameters(operator.parameters)
		return NewFilterIterator(exp, nextIterator)
	}

	panic(fmt.Sprintf("Unknown operator %v", operator))
}

func parseFilterParameters(filterParams []string) BinaryExpression {
	if len(filterParams) != 3 {
		panic("Unable to parse filter parameters, expected 3 parameters")
	}

	switch filterParams[1] {
	case "<":
		return NewLTExpression(filterParams[0], filterParams[2])
	case "=":
		return NewEQExpression(filterParams[0], filterParams[2])
	case ">":
		return NewGTExpression(filterParams[0], filterParams[2])
	}

	panic(fmt.Sprintf("Unknown filter operator %s", filterParams[1]))
}

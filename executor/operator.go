package executor

import (
	"fmt"
	"strconv"
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
	LIMIT
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
	case LIMIT:
		nextIterator := op.parseNextOperator()
		limit := parseLimitParameters(operator.parameters)
		return NewLimitIterator(nextIterator, limit)
	case FILTER:
		nextIterator := op.parseNextOperator()
		exp := parseFilterParameters(operator.parameters)
		return NewFilterIterator(exp, nextIterator)
	}

	panic(fmt.Sprintf("Unknown operator %v", operator))
}

func (op *OperatorParser) parseNextOperator() Iterator {
	nextIterator := op.parseOperator(op.operators[op.currIdx+1])
	op.currIdx++
	return nextIterator
}

// ============================================================================
// Helper Methods
// ============================================================================

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

func parseLimitParameters(limitParameters []string) int {
	if len(limitParameters) > 1 {
		panic(fmt.Sprintf("Limit expected 1 integer parameter, but got: %s", limitParameters))
	}

	i, err := strconv.Atoi(limitParameters[0])
	if err != nil {
		panic(err)
	}

	return i
}

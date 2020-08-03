package executor

import "fmt"

// BinaryExpression is the interface for an expression that returns true or false
type BinaryExpression interface {
	Execute(Tuple) bool
}

// ============================================================================
// LESS THAN
// ============================================================================

// LTExpression is a BinaryExpression that returns whether the left expression
// is less than the right
type LTExpression struct {
	key   string
	value string
}

// NewLTExpression constructs a LTExpression
func NewLTExpression(key, value string) BinaryExpression {
	return &LTExpression{key: key, value: value}
}

// Execute returns the result of applying the LTExpression
func (lt *LTExpression) Execute(tuple Tuple) bool {
	for _, v := range tuple.Values {
		if v.Key == lt.key {
			return v.Value < lt.value
		}
	}

	panic(fmt.Sprintf("tuple: %v did not contain field: %s", tuple, lt.key))
}

// ============================================================================
// GREATER THAN
// ============================================================================

// GTExpression is a BinaryExpression that returns whether the left
// is greater than the right
type GTExpression struct {
	key   string
	value string
}

// NewGTExpression creates a new GTExpression
func NewGTExpression(key, value string) BinaryExpression {
	return &GTExpression{key: key, value: value}
}

// Execute returns the result of applying the GT Expresison condition
func (gt *GTExpression) Execute(tuple Tuple) bool {
	for _, v := range tuple.Values {
		if v.Key == gt.key {
			return v.Value > gt.value
		}
	}

	panic(fmt.Sprintf("tuple: %v did not contain field: %s", tuple, gt.key))
}

// ============================================================================
// EQUAL TO
// ============================================================================

// EQExpression is a BinaryExpression that returns whether the left
// is equal to the right expression
type EQExpression struct {
	key   string
	value string
}

// NewEQExpression creates a new EQExpression
func NewEQExpression(key, value string) BinaryExpression {
	return &EQExpression{key: key, value: value}
}

// Execute returns the result of applying the EQExpresison condition
func (eq *EQExpression) Execute(tuple Tuple) bool {
	for _, v := range tuple.Values {
		if v.Key == eq.key {
			return v.Value == eq.value
		}
	}

	panic(fmt.Sprintf("tuple: %v did not contain field: %s", tuple, eq.key))
}

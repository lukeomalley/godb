package executor

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
	left  string
	right string
}

// NewLTExpression constructs a LTExpression
func NewLTExpression(left, right string) BinaryExpression {
	return &LTExpression{left: left, right: right}
}

// Execute returns the result of applying the LTExpression
func (lt *LTExpression) Execute() bool {
	return lt.left < lt.right
}

// ============================================================================
// GREATER THAN
// ============================================================================

// GTExpression is a BinaryExpression that returns whether the left
// is greater than the right
type GTExpression struct {
	left  string
	right string
}

// NewGTExpression creates a new GTExpression
func NewGTExpression(left, right string) BinaryExpression {
	return &GTExpression{left: left, right: right}
}

// Execute returns the result of applying the GT Expresison condition
func (gt *GTExpression) Execute() bool {
	return gt.left > gt.right
}

// ============================================================================
// EQUAL TO
// ============================================================================

// EQExpression is a BinaryExpression that returns whether the left
// is equal to the right expression
type EQExpression struct {
	left  string
	right string
}

// NewEQExpression creates a new EQExpression
func NewEQExpression(left, right string) BinaryExpression {
	return &GTExpression{left: left, right: right}
}

// Execute returns the result of applying the EQExpresison condition
func (gt *EQExpression) Execute() bool {
	return gt.left == gt.right
}

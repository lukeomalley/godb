package executor

// FilterIterator filters tuples from its child based on its expression
type FilterIterator struct {
	expression BinaryExpression
	child      Iterator
	curr       Tuple
}

// NewFilterIterator constructs a FilterIterator
func NewFilterIterator(exp BinaryExpression, child Iterator) Iterator {
	return &FilterIterator{
		expression: exp,
		child:      child,
	}
}

// Init initializes the FilterIterator and should be called before
// calling Next.
func (fi *FilterIterator) Init() {
	// Initialization
	fi.child.Init()
}

// Next calls Next on its child iterator until it finds a tuple that
// meets the condition. Sets the curr property when it finds a match
func (fi *FilterIterator) Next() bool {
	for fi.child.Next() {
		tuple := fi.child.Execute()
		if f.expression.Execute(tuple) {
			fi.curr = tuple
			return true
		}
	}

	return false
}

// Execute returns the current tuple that meets the condition
func (fi *FilterIterator) Execute() Tuple {

}

// Close handles cleanup
func (fi *FilterIterator) Close() {
	// Cleanup
}

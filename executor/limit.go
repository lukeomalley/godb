package executor

// LimitIterator "Scans" a table and returns one tuple at a time
type LimitIterator struct {
	child       Iterator
	numReturned int
	limit       int
}

// NewLimitIterator constructs a new Scan Iterator
func NewLimitIterator(child Iterator, limit int) Iterator {
	li := &LimitIterator{child: child, limit: limit, numReturned: 0}
	return li
}

// Init initializes the Limit Iterator. Should be called before calling Next.
func (li *LimitIterator) Init() {
	// Initialization
}

// Next returns a boolean stating whether of not the Iterator has more tuples to emit
func (li *LimitIterator) Next() bool {
	return li.child.Next() && li.numReturned < li.limit
}

// Execute returns the scanned tuples in order
func (li *LimitIterator) Execute() Tuple {
	result := li.child.Execute()
	li.numReturned++
	return result
}

// Close performs clean up after the iterator is complete. Should be called
// after all the data has been pulled from the iterator.
func (li *LimitIterator) Close() {
	// Cleanup
}

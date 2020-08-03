package executor

// ScanIterator "Scans" a table and returns one tuple at a time
type ScanIterator struct {
	tuples []Tuple
	idx    int
}

// NewScanIterator constructs a new Scan Iterator
func NewScanIterator(tuples []Tuple) Iterator {
	si := &ScanIterator{tuples: tuples, idx: 0}
	return si
}

// Init initializes the Scan Iterator. Should be called before calling Next.
func (si *ScanIterator) Init() {
	// Initialization
}

// Next returns a boolean stating whether of not the Iterator has more tuples to emit
func (si *ScanIterator) Next() bool {
	return si.idx <= len(si.tuples)-1
}

// Execute returns the scanned tuples in order
func (si *ScanIterator) Execute() Tuple {
	result := si.tuples[si.idx]
	si.idx++
	return result
}

// Close performs clean up after the iterator is complete. Should be called
// after all the data has been pulled from the iterator.
func (si *ScanIterator) Close() {
	// Cleanup
}

package executor

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

// FileWriter writes files in a custom format
type FileWriter struct {
	numRows     int
	columnNames []string
	w           io.Writer
	numWritten  int
	uvarintBuf  []byte
}

// NewFileWriter creates and returns a new file writer
func NewFileWriter(columnNames []string, numRows int, w io.Writer) *FileWriter {
	return &FileWriter{
		columnNames: columnNames,
		numRows:     numRows,
		w:           w,
		uvarintBuf:  make([]byte, binary.MaxVarintLen64),
	}
}

// Append appends a tuple to the file.
func (w *FileWriter) Append(t Tuple) error {
	// Check if the file header has been written
	if w.numWritten == 0 {
		if err := w.writeHeader(); err != nil {
			return fmt.Errorf("Writer: Append: error writing header: %v", err)
		}
	}

	// Check if the tuple has the correct number of columns
	if len(t.Values) != len(w.columnNames) {
		return fmt.Errorf("Writer: Append: tried to write a tuple with %v with %d values, but writer expects: %d columns", t, len(t.Values), len(w.columnNames))
	}

	// Write the tuple
	for _, v := range t.Values {
		if err := w.writeUVarint(uint64(len(v.StringValue))); err != nil {
			return fmt.Errorf("Writer: Append: error writing string length uvarint: %v", err)
		}

		if _, err := w.w.Write([]byte(v.StringValue)); err != nil {
			return fmt.Errorf("Writer: Append: error writing string: %s, err: %v", v, err)
		}

	}
	w.numRows++

	return nil
}

func (w *FileWriter) writeHeader() error {
	header := Header{
		Version:     LatestVersion,
		NumRows:     w.numRows,
		ColumnNames: w.columnNames,
	}

	headerBytes, err := json.Marshal(&header)
	if err != nil {
		return fmt.Errorf("writeHeader: error marshaling header: %v, err: %v", header, err)
	}

	if err := w.writeUVarint(uint64(len(headerBytes))); err != nil {
		return fmt.Errorf("writeHeader: error writing headerBytes uvarint length: %v", err)
	}

	if _, err := w.w.Write(headerBytes); err != nil {
		return fmt.Errorf("writeHeader: error writing headerBytes: %v", err)
	}

	return nil
}

func (w *FileWriter) writeUVarint(x uint64) error {
	varintLen := binary.PutUvarint(w.uvarintBuf, x)
	_, err := w.w.Write(w.uvarintBuf[:varintLen])
	if err != nil {
		return fmt.Errorf("writeUVarint: error writing uvarint: %v", err)
	}

	return nil
}

// Close closes the writer
func (w *FileWriter) Close() error {
	if w.numWritten != w.numWritten {
		return fmt.Errorf("Writer: Close: expected to write: %d rows, but wrote: %d", w.numWritten, w.numRows)
	}

	// No-op for the current iteration
	return nil
}

package executor

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

// FileScanIterator is an iterator that reads the custom file format
type FileScanIterator struct {
	header  *Header
	r       *byteReader
	numRead int
	next    Tuple
}

// NewFileScanIterator creates a new file scan iterator
func NewFileScanIterator(r io.Reader) *FileScanIterator {
	return &FileScanIterator{
		r: newByteReader(r),
	}
}

// Next reads the next tuple and returns true if there is another tuple to be read
func (fs *FileScanIterator) Next() bool {
	if fs.header == nil {
		fs.readHeader()
	}

	if fs.numRead < fs.header.NumRows {
		fs.readTuple()
		return true
	}

	return false
}

// Execute returns the next tuple from the iterator
func (fs *FileScanIterator) Execute() Tuple {
	return fs.next
}

// Reads the header from the file and sets the header variable
func (fs *FileScanIterator) readHeader() {
	headerLength, err := binary.ReadUvarint(fs.r)
	if err != nil {
		panic(fmt.Sprintf("FileScanIterator: error reading header length: %v", err))
	}

	headerBytes := make([]byte, headerLength)
	if _, err := io.ReadFull(fs.r, headerBytes); err != nil {
		panic(fmt.Sprintf("FileScanIterator: error reading header bytes: %v", err))
	}

	header := &Header{}
	if err := json.Unmarshal(headerBytes, header); err != nil {
		panic(fmt.Sprintf("FileScanIterator: error unmarshaling header, %v", err))
	}

	fs.header = header
}

// Reads one tuple from the file and sets the next property
func (fs *FileScanIterator) readTuple() {
	tuple := Tuple{}

	for _, col := range fs.header.ColumnNames {
		valLen, err := binary.ReadUvarint(fs.r)
		if err != nil {
			panic(fmt.Sprintf("FileScanIterator: error reading next tuple value length, %v", err))
		}

		valBytes := make([]byte, valLen)
		if _, err := io.ReadFull(fs.r, valBytes); err != nil {
			panic(fmt.Sprintf("FileScanIterator: error reading next tuple value bytes, %v", err))
		}

		tuple.Values = append(tuple.Values, Value{
			Key:         col,
			Value:       string(valBytes),
			StringValue: string(valBytes),
		})
	}

	fs.next = tuple
}

// byteReader is a wrapper around io.Reader that implements io.ByteReader
type byteReader struct {
	io.Reader
	byteBuf []byte
}

func newByteReader(r io.Reader) *byteReader {
	return &byteReader{
		Reader:  r,
		byteBuf: make([]byte, 1),
	}
}

func (b *byteReader) ReadByte() (byte, error) {
	n, err := b.Reader.Read(b.byteBuf)
	if err != nil {
		return 0, fmt.Errorf("byteReader: ReadByte: error reading byte: %v", err)
	}

	if n != 1 {
		return 0, fmt.Errorf("byteReader: ReadByte: expected to read one byte, but read %d", n)
	}

	return b.byteBuf[0], nil
}

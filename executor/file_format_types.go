package executor

import "errors"

// LatestVersion is the latest version of the file format
const LatestVersion = 1

// Header represents a file header in the custom file format
type Header struct {
	Version     int
	NumRows     int
	ColumnNames []string // "count" the column names to know how many "strings" to read before considering yourself on the next row
}

// Validate validated the header.
func (h *Header) Validate() error {
	if h.Version == 0 {
		return errors.New("Header: Validate: Version must not be 0")
	}

	if h.NumRows == 0 {
		return errors.New("Header: Validate: NumRows must not be 0")
	}

	if len(h.ColumnNames) == 0 {
		return errors.New("Header: Validate: len(ColumnNames) must not be 0")
	}

	return nil
}

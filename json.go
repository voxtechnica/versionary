package versionary

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
)

// ToJSON serializes the provided entity as a JSON byte slice.
func ToJSON[T any](t T) ([]byte, error) {
	return json.Marshal(t)
}

// FromJSON deserializes the provided JSON byte slice into an entity.
func FromJSON[T any](j []byte) (T, error) {
	var t T
	err := json.NewDecoder(bytes.NewReader(j)).Decode(&t)
	return t, err
}

// ToCompressedJSON serializes the provided entity as a gzip-compressed JSON byte slice.
func ToCompressedJSON[T any](entity T) ([]byte, error) {
	// Encode and compress the entity into a buffer.
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if err := json.NewEncoder(gz).Encode(entity); err != nil {
		return nil, fmt.Errorf("failed encoding compressed JSON: %w", err)
	}
	// Flush and close the gzip writer
	if err := gz.Close(); err != nil {
		return nil, fmt.Errorf("failed closing gzip writer: %w", err)
	}
	return buf.Bytes(), nil
}

// FromCompressedJSON deserializes the provided gzip-compressed JSON byte slice into an entity.
func FromCompressedJSON[T any](j []byte) (T, error) {
	var t T
	// Create a gzip reader
	gz, err := gzip.NewReader(bytes.NewReader(j))
	if err != nil {
		return t, fmt.Errorf("failed creating gzip reader: %w", err)
	}
	// Read the uncompressed JSON into the entity
	if err = json.NewDecoder(gz).Decode(&t); err != nil {
		return t, fmt.Errorf("failed decoding compressed JSON: %w", err)
	}
	// Flush and close the gzip reader
	if err = gz.Close(); err != nil {
		return t, fmt.Errorf("failed closing gzip reader: %w", err)
	}
	return t, nil
}

// UncompressJSON converts the provided gzip-compressed JSON byte slice into uncompressed bytes.
func UncompressJSON(j []byte) ([]byte, error) {
	var b bytes.Buffer
	// Create a gzip reader
	gz, err := gzip.NewReader(bytes.NewReader(j))
	if err != nil {
		return nil, fmt.Errorf("failed creating gzip reader: %w", err)
	}
	// Read the uncompressed JSON into the buffer
	if _, err = io.Copy(&b, gz); err != nil {
		return nil, fmt.Errorf("failed uncompressing JSON: %w", err)
	}
	// Flush and close the gzip reader
	if err = gz.Close(); err != nil {
		return nil, fmt.Errorf("failed closing gzip reader: %w", err)
	}
	return b.Bytes(), nil
}

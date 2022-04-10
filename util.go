package versionary

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
)

// Map turns a slice of T1 into a slice of T2 using a mapping function.
func Map[T1, T2 any](s []T1, f func(T1) T2) []T2 {
	r := make([]T2, len(s))
	for i, v := range s {
		r[i] = f(v)
	}
	return r
}

// Reduce reduces a slice of T1 to a single value using a reduction function.
func Reduce[T1, T2 any](s []T1, initializer T2, f func(T2, T1) T2) T2 {
	r := initializer
	for _, v := range s {
		r = f(r, v)
	}
	return r
}

// Filter filters values from a slice using a filter function.
// It returns a new slice with only the elements of s that satisfy the predicate.
func Filter[T any](s []T, f func(T) bool) []T {
	var r []T
	for _, v := range s {
		if f(v) {
			r = append(r, v)
		}
	}
	return r
}

// Contains returns true if the slice contains the provided value.
func Contains[T comparable](s []T, value T) bool {
	for _, v := range s {
		if v == value {
			return true
		}
	}
	return false
}

// Batch divides the provided slice of things into batches of the specified maximum size.
func Batch[T any](items []T, batchSize int) [][]T {
	batches := make([][]T, 0)
	batch := make([]T, 0)
	for _, item := range items {
		batch = append(batch, item)
		if len(batch) == batchSize {
			batches = append(batches, batch)
			batch = make([]T, 0)
		}
	}
	if len(batch) > 0 {
		batches = append(batches, batch)
	}
	return batches
}

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

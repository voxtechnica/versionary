package versionary

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
)

// Map turns a []T1 to a []T2 using a mapping function.
func Map[T1, T2 any](s []T1, f func(T1) T2) []T2 {
	r := make([]T2, len(s))
	for i, v := range s {
		r[i] = f(v)
	}
	return r
}

// Reduce reduces a []T1 to a single value using a reduction function.
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

// Contains returns true if the slice contains the value.
func Contains[T comparable](s []T, value T) bool {
	for _, v := range s {
		if v == value {
			return true
		}
	}
	return false
}

// MakeBatches divides the provided slice of things into batches of the specified maximum size.
func MakeBatches[T any](items []T, batchSize int) [][]T {
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

// ToJSON converts the provided entity to a JSON byte slice.
func ToJSON[T any](entity T) ([]byte, error) {
	return json.Marshal(entity)
}

// ToCompressedJSON converts the provided entity to a JSON string and compresses it.
func ToCompressedJSON[T any](entity T) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if err := json.NewEncoder(gz).Encode(entity); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// FromJSON converts the provided JSON string to an entity.
func FromJSON[T any](jsonStr []byte, entity *T) error {
	return json.NewDecoder(bytes.NewReader(jsonStr)).Decode(entity)
}

// FromCompressedJSON converts the provided compressed JSON string to an entity.
func FromCompressedJSON[T any](gzJSON []byte, entity *T) error {
	gz, err := gzip.NewReader(bytes.NewReader(gzJSON))
	if err != nil {
		return err
	}
	if err := json.NewDecoder(gz).Decode(entity); err != nil {
		return err
	}
	return gz.Close()
}

// UncompressJSON converts the provided compressed bytes to a JSON byte slice.
func UncompressJSON(gzJSON []byte) ([]byte, error) {
	var buf bytes.Buffer
	gz, err := gzip.NewReader(bytes.NewReader(gzJSON))
	if err != nil {
		return nil, err
	}
	if _, err := io.Copy(&buf, gz); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

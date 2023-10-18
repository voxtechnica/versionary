package versionary

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// ErrEmptyFilter is returned when the filter string is empty.
var ErrEmptyFilter = errors.New("empty filter")

// dateRegex is a regular expression that matches a date in the format YYYY-MM-DD.
var dateRegex = regexp.MustCompile(`^\d{4}-(0[1-9]|1[012])-(0[1-9]|[12][0-9]|3[01])$`)

// IsValidDate returns true if the supplied string is a valid date in the format YYYY-MM-DD.
// Note that this function does not validate the actual number of days in a given month.
// For example, it does not indicate that February 31 is invalid.
// It only validates the format, including the maximum value for each field.
func IsValidDate(date string) bool {
	return date != "" && dateRegex.MatchString(date)
}

// TextValue represents a key-value pair where the value is a string.
type TextValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// String returns a string representation of the TextValue.
func (tv TextValue) String() string {
	return tv.Key + ": " + tv.Value
}

// ContainsAny returns true if the Value contains any of the terms (an OR filter).
// The terms should be lowercase for a case-insensitive search.
func (tv TextValue) ContainsAny(terms []string) bool {
	v := strings.ToLower(tv.Value)
	for _, t := range terms {
		if strings.Contains(v, t) {
			return true
		}
	}
	return false
}

// ContainsAll returns true if the Value contains all the terms (an AND filter).
// The terms should be lowercase for a case-insensitive search.
func (tv TextValue) ContainsAll(terms []string) bool {
	v := strings.ToLower(tv.Value)
	for _, t := range terms {
		if !strings.Contains(v, t) {
			return false
		}
	}
	return true
}

// ContainsFilter returns a function that can be used to filter TextValues.
// The case-insensitive contains query is split into words, and the words are compared with the value in the TextValue.
// If anyMatch is true, then a TextValue is included in the results if any of the words are found (OR filter).
// If anyMatch is false, then the TextValue must contain all the words in the query string (AND filter).
func ContainsFilter(contains string, anyMatch bool) (func(tv TextValue) bool, error) {
	terms := strings.Fields(strings.ToLower(contains))
	if len(terms) == 0 {
		return func(tv TextValue) bool { return false }, ErrEmptyFilter
	}
	if anyMatch {
		return func(tv TextValue) bool { return tv.ContainsAny(terms) }, nil
	} else {
		return func(tv TextValue) bool { return tv.ContainsAll(terms) }, nil
	}
}

// TextValuesMap converts a slice of TextValues into a key/value map.
func TextValuesMap(values []TextValue) map[string]string {
	m := make(map[string]string)
	for _, tv := range values {
		m[tv.Key] = tv.Value
	}
	return m
}

// NumValue represents a key-value pair where the value is a number.
type NumValue struct {
	Key   string  `json:"key"`
	Value float64 `json:"value"`
}

// String returns a string representation of the NumValue.
func (nv NumValue) String() string {
	return fmt.Sprintf("%s: %g", nv.Key, nv.Value)
}

// NumValuesMap converts a slice of NumValues into a key/value map.
func NumValuesMap(values []NumValue) map[string]float64 {
	m := make(map[string]float64)
	for _, nv := range values {
		m[nv.Key] = nv.Value
	}
	return m
}

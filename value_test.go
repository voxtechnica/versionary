package versionary

import (
	"testing"
)

func TestIsValidDate(t *testing.T) {
	tests := []struct {
		name string
		date string
		want bool
	}{
		{name: "valid date", date: "2023-01-01", want: true},
		{name: "invalid date", date: "2023-13-01", want: false},
		{name: "invalid format", date: "01-31-2023", want: false},
		{name: "not a date", date: "invalid", want: false},
		{name: "empty date", date: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsValidDate(tt.date); got != tt.want {
				t.Errorf("IsValidDate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTextValue_ContainsAny(t *testing.T) {
	tv := TextValue{
		Key:   "test",
		Value: "This is a test string",
	}

	terms := []string{"test", "foo", "bar"}
	if !tv.ContainsAny(terms) {
		t.Errorf("Expected ContainsAny to return true, but it returned false")
	}

	terms = []string{"foo", "bar"}
	if tv.ContainsAny(terms) {
		t.Errorf("Expected ContainsAny to return false, but it returned true")
	}
}

func TestTextValue_ContainsAll(t *testing.T) {
	tv := TextValue{
		Key:   "test",
		Value: "This is a test string",
	}

	terms := []string{"test", "string"}
	if !tv.ContainsAll(terms) {
		t.Errorf("Expected ContainsAll to return true, but it returned false")
	}

	terms = []string{"test", "foo"}
	if tv.ContainsAll(terms) {
		t.Errorf("Expected ContainsAll to return false, but it returned true")
	}
}

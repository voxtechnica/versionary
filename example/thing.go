// Package thing provides an example demonstrating a way of using the versionary package.
// It includes a simple struct, Thing, and a few methods that operate on it.
// It also includes a Table and a Service that can be used to manage Things.
package thing

import (
	"time"

	"github.com/voxtechnica/tuid-go"
	v "github.com/voxtechnica/versionary"
)

// Thing is a simple example struct that demonstrates how to use the versionary package.
type Thing struct {
	ID        string    `json:"id"`
	VersionID string    `json:"versionId"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
	ExpiresAt time.Time `json:"expiresAt"`
	Tags      []string  `json:"tags"`
	Count     int       `json:"count"`
	Message   string    `json:"message"`
}

// CompressedJSON returns a compressed JSON representation of the Thing.
// It fails silently, returning nil, if the JSON cannot be generated.
func (t Thing) CompressedJSON() []byte {
	j, err := v.ToCompressedJSON(t)
	if err != nil {
		return nil
	}
	return j
}

// CreatedOn returns an ISO-8601 formatted string date from the CreatedAt timestamp.
// It is useful for grouping things by date.
func (t Thing) CreatedOn() string {
	if t.CreatedAt.IsZero() {
		return ""
	}
	return t.CreatedAt.Format("2006-01-02")
}

// Validate checks whether the Thing has all required fields and
// whether the supplied fields are valid, returning a list of problems.
// If the list is empty, the Thing is valid.
func (t Thing) Validate() []string {
	var problems []string
	if t.ID == "" || !tuid.IsValid(tuid.TUID(t.ID)) {
		problems = append(problems, "ID is missing or invalid")
	}
	if t.VersionID == "" || !tuid.IsValid(tuid.TUID(t.VersionID)) {
		problems = append(problems, "VersionID is missing or invalid")
	}
	if t.CreatedAt.IsZero() {
		problems = append(problems, "CreatedAt is missing")
	}
	if t.UpdatedAt.IsZero() {
		problems = append(problems, "UpdatedAt is missing")
	}
	if t.ExpiresAt.IsZero() {
		problems = append(problems, "ExpiresAt is missing")
	}
	if t.Message == "" {
		problems = append(problems, "Message is missing")
	}
	return problems
}

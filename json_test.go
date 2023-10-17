package versionary

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/voxtechnica/tuid-go"
)

type entity struct {
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"createdAt"`
	Title     string    `json:"title"`
	Tags      []string  `json:"tags"`
	Count     int       `json:"count"`
}

func TestJSON(t *testing.T) {
	expect := assert.New(t)
	id := tuid.NewID()
	at, _ := id.Time()
	e1 := entity{
		ID:        id.String(),
		CreatedAt: at,
		Title:     "abc",
		Tags:      []string{"tag1", "tag2"},
		Count:     1,
	}
	// Serialize to JSON bytes
	j, err := ToJSON(e1)
	if expect.NoError(err) {
		expect.NotEmpty(j)
		// Deserialize to entity
		e2, err := FromJSON[entity](j)
		if expect.NoError(err) {
			expect.Equal(e1, e2)
		}
	}
}

func TestCompressedJSON(t *testing.T) {
	expect := assert.New(t)
	id := tuid.NewID()
	at, _ := id.Time()
	e1 := entity{
		ID:        id.String(),
		CreatedAt: at,
		Title:     "abc",
		Tags:      []string{"tag1", "tag2"},
		Count:     1,
	}
	// Serialize to compressed JSON bytes
	j, err := ToCompressedJSON(e1)
	if expect.NoError(err) {
		expect.NotEmpty(j)
		// Deserialize to entity
		e2, err := FromCompressedJSON[entity](j)
		if expect.NoError(err) {
			expect.Equal(e1, e2)
			// Deserialize to JSON bytes
			j2, err := UncompressJSON(j)
			if expect.NoError(err) {
				expect.NotEmpty(j2)
				expect.Contains(string(j2), `"id":"`+id.String()+`"`)
			}
		}
	}
}

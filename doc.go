// Package versionary provides an opinionated way of managing versioned entities in a NoSQL database,
// specifically AWS DynamoDB. It uses a "wide row" schema design to provide fast access to denormalized data.
//
// The package revolves around two main components:
// 1. TableRow: A configuration describing how an entity should be partitioned and sorted in DynamoDB.
// 2. Table (and MemTable): The data access layer used to write and read entities.
//
// Entities are expected to have a unique ID, and each revision generates a new unique VersionID.
// Time-based Unique Identifiers (TUID) are recommended for both IDs and VersionIDs so that alphabetical
// sorting provides chronological ordering.
//
// Usage Example:
//
//	package main
//
//	import (
//		"context"
//		"time"
//		v "github.com/voxtechnica/versionary"
//	)
//
//	type Thing struct {
//		ID        string    `json:"id"`
//		VersionID string    `json:"versionId"`
//		CreatedAt time.Time `json:"createdAt"`
//		Message   string    `json:"message"`
//	}
//
//	// Provide compressed JSON representation
//	func (t Thing) CompressedJSON() []byte {
//		j, _ := v.ToCompressedJSON(t)
//		return j
//	}
//
//	// Define the EntityRow for storing all versions
//	var rowThingsVersion = v.TableRow[Thing]{
//		RowName:      "things_version",
//		PartKeyName:  "id",
//		PartKeyValue: func(t Thing) string { return t.ID },
//		SortKeyName:  "version_id",
//		SortKeyValue: func(t Thing) string { return t.VersionID },
//		JsonValue:    func(t Thing) []byte { return t.CompressedJSON() },
//	}
//
//	func main() {
//		// Initialize Table Configuration
//		table := v.Table[Thing]{
//			EntityType: "Thing",
//			TableName:  "things_dev",
//			EntityRow:  rowThingsVersion,
//		}
//
//		// Create an in-memory table for rapid local testing
//		memTable := v.NewMemTable(table)
//
//		ctx := context.Background()
//
//		// Write a new entity
//		thing := Thing{
//			ID:        "1",
//			VersionID: "1",
//			CreatedAt: time.Now(),
//			Message:   "Hello World",
//		}
//		_ = memTable.WriteEntity(ctx, thing)
//
//		// Read the entity
//		_, _ = memTable.ReadEntity(ctx, "1")
//	}
package versionary

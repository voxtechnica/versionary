# Versionary Example

This directory contains a complete, working example of how to build a service using `versionary`. It demonstrates how to define a domain model, configure `TableRow` mappings for DynamoDB, and build a CRUD service around it.

## Files Overview

### `thing.go`
This file defines the domain model: `Thing`.
- It includes standard entity fields like `ID`, `VersionID`, `CreatedAt`, and `UpdatedAt`.
- It provides a `CompressedJSON()` method to satisfy the need for compressed JSON storage in the database, utilizing `versionary.ToCompressedJSON`.
- It includes basic validation logic.

### `things.go`
This file defines the persistence layer using `versionary`. It bridges the gap between the `Thing` struct and DynamoDB wide rows.

Key components defined here:
- **`rowThingsVersion`**: The primary entity row. It stores all versions of a `Thing`, partitioned by `ID` and sorted by `VersionID`.
- **`rowThingsDate`**: An index row that groups current versions of `Thing` by their `CreatedOn` date.
- **`rowThingsTag`**: Another index row that groups current versions of `Thing` by their `Tags`.
- **`NewTable`**: Initializes the `versionary.Table` configuration, registering the entity row and index rows.
- **`Service`**: A struct that wraps `TableReadWriter[Thing]` to provide a clean, domain-specific API (like `Create`, `Update`, `ReadThingsByDate`, etc.) instead of exposing raw table operations.

### `things_test.go`
This file contains the test suite. It heavily relies on `versionary.MemTable`, proving that you can write extensive tests for your service logic and queries without needing a live DynamoDB instance.

## Why Time-based Unique Identifiers (TUID)?

Notice that this example uses [TUID](https://github.com/voxtechnica/tuid-go) for `ID` and `VersionID`. Because TUIDs are lexicographically sortable by their embedded timestamps, sorting by `VersionID` automatically provides chronological ordering of revisions. This is a crucial pattern when working with `versionary`.

## Running the Tests

You can run the tests in this directory using standard Go tooling:

```bash
go test -v ./example/...
```

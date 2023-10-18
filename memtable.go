package versionary

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
)

// MemTable represents a single in-memory table that stores all the "wide rows" for a given entity. Denormalized
// entity data are stored in a single table, reusing the attribute names indicated in the TableRow definition.
// The EntityRow is special; it contains the revision history for the entity, whereas the IndexRows contain only
// values from the latest version of the entity. MemTable implements the TableWriter and TableReader interfaces
// and is intended to be used for testing purposes only.
type MemTable[T any] struct {
	Records    *RecordSet
	EntityType string
	TableName  string
	TTL        bool
	EntityRow  TableRow[T]
	IndexRows  map[string]TableRow[T]
}

// NewMemTable creates a new MemTable from a DynamoDB table definition.
func NewMemTable[T any](table Table[T]) MemTable[T] {
	return MemTable[T]{
		Records:    &RecordSet{},
		EntityType: table.EntityType,
		TableName:  table.TableName,
		TTL:        table.TTL,
		EntityRow:  table.EntityRow,
		IndexRows:  table.IndexRows,
	}
}

// IsValid returns true if the MemTable fields and rows are valid.
func (table MemTable[T]) IsValid() bool {
	for _, row := range table.IndexRows {
		if !row.IsValid() {
			return false
		}
	}
	return table.Records != nil && table.EntityType != "" && table.TableName != "" && table.EntityRow.IsValid()
}

// TableExists returns true if the in-memory table exists.
func (table MemTable[T]) TableExists() bool {
	return table.Records != nil
}

// GetEntityType returns the name of the entity type for the MemTable.
func (table MemTable[T]) GetEntityType() string {
	return table.EntityType
}

// GetTableName returns the name of the table.
func (table MemTable[T]) GetTableName() string {
	return table.TableName
}

// GetRow returns the specified row definition.
func (table MemTable[T]) GetRow(rowName string) (TableRow[T], bool) {
	if rowName == table.EntityRow.RowName {
		return table.EntityRow, true
	}
	row, ok := table.IndexRows[rowName]
	return row, ok
}

// GetEntityRow returns the entity row definition. This row contains the revision history for the entity.
func (table MemTable[T]) GetEntityRow() TableRow[T] {
	return table.EntityRow
}

// EntityID returns the unique entity ID for the provided entity.
func (table MemTable[T]) EntityID(entity T) string {
	if table.EntityRow.PartKeyValue == nil {
		return ""
	}
	return table.EntityRow.PartKeyValue(entity)
}

// EntityVersionID returns the unique entity version ID for the provided entity.
func (table MemTable[T]) EntityVersionID(entity T) string {
	if table.EntityRow.SortKeyValue == nil {
		return ""
	}
	return table.EntityRow.SortKeyValue(entity)
}

// EntityReferenceID returns a unique Reference ID (EntityType-ID-VersionID) for the provided entity.
func (table MemTable[T]) EntityReferenceID(entity T) string {
	entityID := table.EntityID(entity)
	versionID := table.EntityVersionID(entity)
	if entityID == "" || versionID == "" {
		return ""
	}
	if entityID == versionID {
		return table.EntityType + "-" + table.EntityID(entity)
	}
	return table.EntityType + "-" + table.EntityID(entity) + "-" + table.EntityVersionID(entity)
}

// EntityExists checks if an entity exists in the table.
func (table MemTable[T]) EntityExists(ctx context.Context, entityID string) bool {
	if table.Records == nil {
		table.Records = &RecordSet{}
	}
	return table.Records.RecordsExist(table.EntityRow.getRowPartKeyValue(entityID))
}

// EntityVersionExists checks if an entity version exists in the table.
func (table MemTable[T]) EntityVersionExists(ctx context.Context, entityID string, versionID string) bool {
	if table.Records == nil {
		table.Records = &RecordSet{}
	}
	return table.Records.RecordExists(table.EntityRow.getRowPartKeyValue(entityID), versionID)
}

// writeRecords writes the provided records to the in-memory table.
func (table MemTable[T]) writeRecords(records []Record) {
	if table.Records == nil {
		table.Records = &RecordSet{}
	}
	table.Records.SetRecords(records)
}

// deleteRecords deletes the provided records from the in-memory table.
func (table MemTable[T]) deleteRecords(records []Record) {
	if table.Records == nil {
		table.Records = &RecordSet{}
	}
	table.Records.DeleteRecords(records)
}

// WriteEntity batch-writes the provided entity to each of the wide rows in the table.
func (table MemTable[T]) WriteEntity(ctx context.Context, entity T) error {
	records := table.EntityRow.getWriteRecords(entity)
	for _, row := range table.IndexRows {
		records = append(records, row.getWriteRecords(entity)...)
	}
	table.writeRecords(records)
	return nil
}

// UpdateEntity batch-writes the provided entity to each of the wide rows in the table,
// including deleting any obsolete values in index rows.
func (table MemTable[T]) UpdateEntity(ctx context.Context, entity T) error {
	// Fetch the current (obsolete) version of the entity
	entityID := table.EntityID(entity)
	oldVersion, err := table.ReadEntity(ctx, entityID)
	if err == ErrNotFound {
		// If the entity doesn't exist, treat it as a new entity
		return table.WriteEntity(ctx, entity)
	} else if err != nil {
		return fmt.Errorf("failed read to update %s-%s: %w", table.EntityType, entityID, err)
	}
	// Update the entity with the new version
	return table.UpdateEntityVersion(ctx, oldVersion, entity)
}

// UpdateEntityVersion batch-writes the provided entity to each of the wide rows in the table,
// including deleting any obsolete values in index rows.
func (table MemTable[T]) UpdateEntityVersion(ctx context.Context, oldVersion T, newVersion T) error {
	// Generate deletion requests for deleted index row partition keys
	var records []Record
	for _, row := range table.IndexRows {
		deletedKeys := row.getDeletedPartKeyValues(oldVersion, newVersion)
		sortKeyValue := row.SortKeyValue(oldVersion)
		records = append(records, row.getDeleteRecordsForKeys(deletedKeys, sortKeyValue)...)
	}
	table.deleteRecords(records)
	// Generate write requests for the new version of the entity
	records = table.EntityRow.getWriteRecords(newVersion)
	for _, row := range table.IndexRows {
		records = append(records, row.getWriteRecords(newVersion)...)
	}
	table.writeRecords(records)
	return nil
}

// DeleteEntity deletes the provided entity from the table (including all versions).
func (table MemTable[T]) DeleteEntity(ctx context.Context, entity T) error {
	entityID := table.EntityID(entity)
	// Delete index row items based on the current version of the entity
	var records []Record
	for _, row := range table.IndexRows {
		records = append(records, row.getDeleteRecords(entity)...)
	}
	// Delete the entity ID from the list of entity IDs
	records = append(records, Record{
		PartKeyValue: table.EntityRow.getRowPartKey(),
		SortKeyValue: entityID,
	})
	// Delete the batch
	table.deleteRecords(records)
	// Batch-delete all versions of the entity from the EntityRow
	return table.DeleteRow(ctx, table.EntityRow, entityID)
}

// DeleteEntityWithID deletes the specified entity from the table (including all versions).
func (table MemTable[T]) DeleteEntityWithID(ctx context.Context, entityID string) (T, error) {
	entity, err := table.ReadEntity(ctx, entityID)
	if err == ErrNotFound {
		return entity, err
	} else if err != nil {
		return entity, fmt.Errorf("failed read to delete %s-%s: %w", table.EntityType, entityID, err)
	}
	return entity, table.DeleteEntity(ctx, entity)
}

// DeleteEntityVersionWithID deletes the specified version of the entity from the table.
func (table MemTable[T]) DeleteEntityVersionWithID(ctx context.Context, entityID, versionID string) (T, error) {
	// Does the entity version exist?
	entity, err := table.ReadEntityVersion(ctx, entityID, versionID)
	if err == ErrNotFound {
		return entity, err
	} else if err != nil {
		return entity, fmt.Errorf("failed read version to delete %s-%s-%s: %w", table.EntityType, entityID, versionID, err)
	}
	// If the specified version is the current version, then we need to update the index rows.
	currentVersionID, err := table.ReadCurrentEntityVersionID(ctx, entityID)
	if err != nil {
		return entity, fmt.Errorf("failed read current version ID to delete %s-%s-%s: %w", table.EntityType, entityID, versionID, err)
	}
	if currentVersionID == versionID {
		// It's the current version. Is there a previous version?
		var versionIDs []string
		versionIDs, err = table.ReadEntityVersionIDs(ctx, entityID, true, 1, currentVersionID)
		if err != nil {
			return entity, fmt.Errorf("failed read previous version ID to delete %s-%s-%s: %w", table.EntityType, entityID, versionID, err)
		}
		// If there is no previous version, just delete the entity
		if len(versionIDs) == 0 {
			err = table.DeleteEntity(ctx, entity)
			if err != nil {
				return entity, fmt.Errorf("failed to delete entity %s-%s-%s: %w", table.EntityType, entityID, versionID, err)
			}
			return entity, nil
		}
		// Update the entity index rows to the previous version
		var previousVersion T
		previousVersion, err = table.ReadEntityVersion(ctx, entityID, versionIDs[0])
		if err != nil {
			return entity, fmt.Errorf("failed read previous version to delete %s-%s-%s: %w", table.EntityType, entityID, versionID, err)
		}
		err = table.UpdateEntityVersion(ctx, entity, previousVersion)
		if err != nil {
			return entity, fmt.Errorf("failed update previous version to delete %s-%s-%s: %w", table.EntityType, entityID, versionID, err)
		}
	}
	// Delete the specified version of the entity
	partKey := table.EntityRow.getRowPartKeyValue(entityID)
	err = table.DeleteItem(ctx, partKey, versionID)
	if err != nil {
		return entity, fmt.Errorf("failed to delete %s-%s-%s: %w", table.EntityType, entityID, versionID, err)
	}
	return entity, nil
}

// DeleteRow deletes the row for the specified partition key value.
func (table MemTable[T]) DeleteRow(ctx context.Context, row TableRow[T], partKeyValue string) error {
	if partKeyValue == "" {
		return ErrNotFound
	}
	partKey := row.getRowPartKeyValue(partKeyValue)
	if table.Records == nil {
		table.Records = &RecordSet{}
	}
	table.Records.DeleteRecordsForKey(partKey)
	return nil
}

// DeleteItem deletes the item with the specified row partition key and sort key.
// Note that this method will seldom be used. It is primarily for cleaning up obsolete items.
func (table MemTable[T]) DeleteItem(ctx context.Context, rowPartKeyValue string, sortKeyValue string) error {
	if rowPartKeyValue == "" || sortKeyValue == "" {
		return ErrNotFound
	}
	if table.Records == nil {
		table.Records = &RecordSet{}
	}
	table.Records.DeleteRecordForKeys(rowPartKeyValue, sortKeyValue)
	return nil
}

// CountPartKeyValues counts the total number of partition key values for the specified row.
func (table MemTable[T]) CountPartKeyValues(ctx context.Context, row TableRow[T]) (int64, error) {
	return table.CountSortKeyValues(ctx, row, "")
}

// ReadAllPartKeyValues reads all the values of the partition keys used for the specified row.
// Note that for some rows, this may be a very large number of values.
func (table MemTable[T]) ReadAllPartKeyValues(ctx context.Context, row TableRow[T]) ([]string, error) {
	return table.ReadAllSortKeyValues(ctx, row, "")
}

// ReadPartKeyValues reads paginated partition key values for the specified row.
func (table MemTable[T]) ReadPartKeyValues(ctx context.Context, row TableRow[T], reverse bool, limit int, offset string) ([]string, error) {
	return table.ReadSortKeyValues(ctx, row, "", reverse, limit, offset)
}

// ReadPartKeyValueRange reads a range of partition key values for the specified row,
// where the partition key values range between the specified inclusive 'from' and 'to' values.
// If either 'from' or 'to' are empty, they will be replaced with the appropriate sentinel value.
func (table MemTable[T]) ReadPartKeyValueRange(ctx context.Context, row TableRow[T], from string, to string, reverse bool) ([]string, error) {
	return table.ReadSortKeyValueRange(ctx, row, "", from, to, reverse)
}

// CountSortKeyValues counts the total number of sort key values for the specified row and partition key.
func (table MemTable[T]) CountSortKeyValues(ctx context.Context, row TableRow[T], partKeyValue string) (int64, error) {
	var partKey string
	if partKeyValue == "" {
		// Read partition key values
		partKey = row.getRowPartKey()
	} else {
		// Read sort key values
		partKey = row.getRowPartKeyValue(partKeyValue)
	}
	if table.Records == nil {
		table.Records = &RecordSet{}
	}
	return table.Records.CountSortKeys(partKey), nil
}

// ReadAllSortKeyValues reads all the sort key values for the specified row and partition key value.
// Note that for some rows, this may be a very large number of values.
func (table MemTable[T]) ReadAllSortKeyValues(ctx context.Context, row TableRow[T], partKeyValue string) ([]string, error) {
	var partKey string
	if partKeyValue == "" {
		// Read partition key values
		partKey = row.getRowPartKey()
	} else {
		// Read sort key values
		partKey = row.getRowPartKeyValue(partKeyValue)
	}
	if table.Records == nil {
		table.Records = &RecordSet{}
	}
	return table.Records.GetSortKeys(partKey), nil
}

// ReadSortKeyValues reads paginated sort key values for the specified row and partition key value,
// where the sort key values are returned in ascending or descending order. The offset is the last
// sort key value returned in the previous page of results. If the offset is empty, it will be
// replaced with the appropriate sentinel value.
func (table MemTable[T]) ReadSortKeyValues(ctx context.Context, row TableRow[T], partKeyValue string, reverse bool, limit int, offset string) ([]string, error) {
	var partKey string
	if partKeyValue == "" {
		// Read partition key values
		partKey = row.getRowPartKey()
	} else {
		// Read sort key values
		partKey = row.getRowPartKeyValue(partKeyValue)
	}
	if offset == "" {
		if reverse {
			offset = "|" // after letters
		} else {
			offset = "-" // before numbers
		}
	}
	all := table.Records.GetSortKeys(partKey)
	var values []string
	for i := 0; i < len(all) && len(values) < limit; i++ {
		if reverse {
			v := all[len(all)-i-1]
			if v < offset {
				values = append(values, v)
			}
		} else {
			v := all[i]
			if v > offset {
				values = append(values, v)
			}
		}
	}
	return values, nil
}

// ReadSortKeyValueRange reads a range of sort key values for the specified row and partition key value,
// where the sort key values range between the specified inclusive 'from' and 'to' values.
// If either 'from' or 'to' are empty, they will be replaced with the appropriate sentinel value.
func (table MemTable[T]) ReadSortKeyValueRange(ctx context.Context, row TableRow[T], partKeyValue string, from string, to string, reverse bool) ([]string, error) {
	var partKey string
	if partKeyValue == "" {
		// Read partition key values
		partKey = row.getRowPartKey()
	} else {
		// Read sort key values
		partKey = row.getRowPartKeyValue(partKeyValue)
	}
	// Set sentinel values, as applicable
	if from == "" {
		if reverse {
			from = "|" // after letters
		} else {
			from = "-" // before numbers
		}
	}
	if to == "" {
		if reverse {
			to = "-" // before numbers
		} else {
			to = "|" // after letters
		}
	}
	// For the range test, 'from' must be less than 'to'
	f := from
	t := to
	if from > to {
		f = to
		t = from
	}
	if table.Records == nil {
		table.Records = &RecordSet{}
	}
	all := table.Records.GetSortKeys(partKey)
	var values []string
	for i := 0; i < len(all); i++ {
		v := all[i]
		if reverse {
			v = all[len(all)-i-1]
		}
		if v >= f && v <= t {
			values = append(values, v)
		}
	}
	return values, nil
}

// ReadFirstSortKeyValue reads the first sort key value for the specified row and partition key value.
// This method is typically used for looking up an ID corresponding to a foreign key.
func (table MemTable[T]) ReadFirstSortKeyValue(ctx context.Context, row TableRow[T], partKeyValue string) (string, error) {
	keyValues, err := table.ReadAllSortKeyValues(ctx, row, partKeyValue)
	if err != nil || len(keyValues) == 0 {
		return "", ErrNotFound
	}
	return keyValues[0], nil
}

// ReadLastSortKeyValue reads the last sort key value for the specified row and partition key value.
// This method is typically used for looking up the current version ID of a versioned entity.
func (table MemTable[T]) ReadLastSortKeyValue(ctx context.Context, row TableRow[T], partKeyValue string) (string, error) {
	keyValues, err := table.ReadAllSortKeyValues(ctx, row, partKeyValue)
	if err != nil || len(keyValues) == 0 {
		return "", ErrNotFound
	}
	return keyValues[len(keyValues)-1], nil
}

// ReadAllEntityIDs reads all entity IDs from the EntityRow.
// Note that this can return a very large number of IDs!
func (table MemTable[T]) ReadAllEntityIDs(ctx context.Context) ([]string, error) {
	return table.ReadAllPartKeyValues(ctx, table.EntityRow)
}

// ReadEntityIDs reads paginated entity IDs from the EntityRow.
func (table MemTable[T]) ReadEntityIDs(ctx context.Context, reverse bool, limit int, offset string) ([]string, error) {
	return table.ReadPartKeyValues(ctx, table.EntityRow, reverse, limit, offset)
}

// ReadEntityIDRange reads a range of entity IDs from the EntityRow,
// where the entity IDs range between the specified inclusive 'from' and 'to' values.
// If either 'from' or 'to' are empty, they will be replaced with the appropriate sentinel value.
func (table MemTable[T]) ReadEntityIDRange(ctx context.Context, from string, to string, reverse bool) ([]string, error) {
	return table.ReadPartKeyValueRange(ctx, table.EntityRow, from, to, reverse)
}

// ReadAllEntityVersionIDs reads all entity version IDs for the specified entity.
// Note that this can return a very large number of IDs!
func (table MemTable[T]) ReadAllEntityVersionIDs(ctx context.Context, entityID string) ([]string, error) {
	return table.ReadAllSortKeyValues(ctx, table.EntityRow, entityID)
}

// ReadEntityVersionIDs reads paginated entity version IDs for the specified entity.
func (table MemTable[T]) ReadEntityVersionIDs(ctx context.Context, entityID string, reverse bool, limit int, offset string) ([]string, error) {
	return table.ReadSortKeyValues(ctx, table.EntityRow, entityID, reverse, limit, offset)
}

// ReadCurrentEntityVersionID reads the current entity version ID for the specified entity.
func (table MemTable[T]) ReadCurrentEntityVersionID(ctx context.Context, entityID string) (string, error) {
	return table.ReadLastSortKeyValue(ctx, table.EntityRow, entityID)
}

// ReadEntities reads the current versions of the specified entities from the EntityRow.
// If an entity does not exist, it will be omitted from the results.
func (table MemTable[T]) ReadEntities(ctx context.Context, entityIDs []string) []T {
	entities := make([]T, 0)
	for _, entityID := range entityIDs {
		if entity, err := table.ReadEntity(ctx, entityID); err == nil {
			entities = append(entities, entity)
		}
	}
	return entities
}

// ReadEntitiesAsJSON reads the current versions of the specified entities from the EntityRow as a JSON byte slice.
// If an entity does not exist, it will be omitted from the results.
func (table MemTable[T]) ReadEntitiesAsJSON(ctx context.Context, entityIDs []string) []byte {
	var entities bytes.Buffer
	entities.WriteString("[")
	var i int
	for _, entityID := range entityIDs {
		if entity, err := table.ReadEntityAsJSON(ctx, entityID); err == nil {
			if i > 0 {
				entities.WriteString(",")
			}
			entities.Write(entity)
			i++
		}
	}
	entities.WriteString("]")
	return entities.Bytes()
}

// ReadEntity reads the current version of the specified entity.
func (table MemTable[T]) ReadEntity(ctx context.Context, entityID string) (T, error) {
	gzJSON, err := table.ReadEntityAsCompressedJSON(ctx, entityID)
	if err != nil {
		return *new(T), err // e.g. ErrNotFound
	}
	entity, err := FromCompressedJSON[T](gzJSON)
	if err != nil {
		return entity, fmt.Errorf("failed to read %s-%s: %w", table.EntityType, entityID, err)
	}
	return entity, nil
}

// ReadEntityAsJSON reads the current version of the specified entity as a JSON byte slice.
func (table MemTable[T]) ReadEntityAsJSON(ctx context.Context, entityID string) ([]byte, error) {
	gzJSON, err := table.ReadEntityAsCompressedJSON(ctx, entityID)
	if err != nil {
		return nil, err // e.g. ErrNotFound
	}
	j, err := UncompressJSON(gzJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s-%s: %w", table.EntityType, entityID, err)
	}
	return j, nil
}

// ReadEntityAsCompressedJSON reads the current version of the specified entity, returning a compressed JSON byte slice.
func (table MemTable[T]) ReadEntityAsCompressedJSON(ctx context.Context, entityID string) ([]byte, error) {
	if table.EntityRow.JsonValue == nil {
		return nil, errors.New("the EntityRow must contain compressed JSON values")
	}
	if entityID == "" {
		return nil, ErrNotFound
	}
	versionID, err := table.ReadCurrentEntityVersionID(ctx, entityID)
	if err == ErrNotFound {
		return nil, err
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read %s-%s: %w", table.EntityType, entityID, err)
	}
	if versionID == "" {
		return nil, ErrNotFound
	}
	record, ok := table.Records.GetRecord(table.EntityRow.getRowPartKeyValue(entityID), versionID)
	if !ok {
		return nil, ErrNotFound
	}
	return record.JsonValue, nil
}

// ReadEntityVersion reads the specified version of the specified entity.
func (table MemTable[T]) ReadEntityVersion(ctx context.Context, entityID string, versionID string) (T, error) {
	return table.ReadEntityFromRow(ctx, table.EntityRow, entityID, versionID)
}

// ReadEntityVersionAsJSON reads the specified version of the specified entity as a JSON byte slice.
func (table MemTable[T]) ReadEntityVersionAsJSON(ctx context.Context, entityID string, versionID string) ([]byte, error) {
	return table.ReadEntityFromRowAsJSON(ctx, table.EntityRow, entityID, versionID)
}

// ReadEntityVersions reads paginated versions of the specified entity.
func (table MemTable[T]) ReadEntityVersions(ctx context.Context, entityID string, reverse bool, limit int, offset string) ([]T, error) {
	return table.ReadEntitiesFromRow(ctx, table.EntityRow, entityID, reverse, limit, offset)
}

// ReadEntityVersionsAsJSON reads paginated versions of the specified entity as a JSON byte slice.
func (table MemTable[T]) ReadEntityVersionsAsJSON(ctx context.Context, entityID string, reverse bool, limit int, offset string) ([]byte, error) {
	return table.ReadEntitiesFromRowAsJSON(ctx, table.EntityRow, entityID, reverse, limit, offset)
}

// ReadAllEntityVersions reads all versions of the specified entity.
// Note: this can return a very large number of versions! Use with caution.
func (table MemTable[T]) ReadAllEntityVersions(ctx context.Context, entityID string) ([]T, error) {
	return table.ReadAllEntitiesFromRow(ctx, table.EntityRow, entityID)
}

// ReadAllEntityVersionsAsJSON reads all versions of the specified entity as a JSON byte slice.
// Note: this can return a very large number of versions! Use with caution.
func (table MemTable[T]) ReadAllEntityVersionsAsJSON(ctx context.Context, entityID string) ([]byte, error) {
	return table.ReadAllEntitiesFromRowAsJSON(ctx, table.EntityRow, entityID)
}

// ReadEntityFromRow reads the specified entity from the specified row.
func (table MemTable[T]) ReadEntityFromRow(ctx context.Context, row TableRow[T], partKeyValue string, sortKeyValue string) (T, error) {
	gzJSON, err := table.ReadEntityFromRowAsCompressedJSON(ctx, row, partKeyValue, sortKeyValue)
	if err != nil {
		return *new(T), err // e.g. ErrNotFound
	}
	entity, err := FromCompressedJSON[T](gzJSON)
	if err != nil {
		return entity, fmt.Errorf("error reading %s %s %s: %w",
			table.EntityType, row.getRowPartKeyValue(partKeyValue), sortKeyValue, err)
	}
	return entity, nil
}

// ReadEntityFromRowAsJSON reads the specified entity from the specified row as JSON bytes.
func (table MemTable[T]) ReadEntityFromRowAsJSON(ctx context.Context, row TableRow[T], partKeyValue string, sortKeyValue string) ([]byte, error) {
	gzJSON, err := table.ReadEntityFromRowAsCompressedJSON(ctx, row, partKeyValue, sortKeyValue)
	if err != nil {
		return nil, err // e.g. ErrNotFound
	}
	j, err := UncompressJSON(gzJSON)
	if err != nil {
		return nil, fmt.Errorf("error reading %s %s %s: %w",
			table.EntityType, row.getRowPartKeyValue(partKeyValue), sortKeyValue, err)
	}
	return j, nil
}

// ReadEntityFromRowAsCompressedJSON reads the specified entity from the specified row as compressed JSON bytes.
func (table MemTable[T]) ReadEntityFromRowAsCompressedJSON(ctx context.Context, row TableRow[T], partKeyValue string, sortKeyValue string) ([]byte, error) {
	if row.JsonValue == nil {
		return nil, errors.New("row " + row.RowName + " must contain compressed JSON values")
	}
	r, ok := table.Records.GetRecord(row.getRowPartKeyValue(partKeyValue), sortKeyValue)
	if !ok {
		return nil, ErrNotFound
	}
	return r.JsonValue, nil
}

// ReadEntitiesFromRow reads paginated entities from the specified row.
func (table MemTable[T]) ReadEntitiesFromRow(ctx context.Context, row TableRow[T], partKeyValue string, reverse bool, limit int, offset string) ([]T, error) {
	entities := make([]T, 0)
	if row.JsonValue == nil {
		return entities, errors.New("row " + row.RowName + " must contain compressed JSON values")
	}
	if partKeyValue == "" {
		return entities, nil
	}
	sortKeyValues, err := table.ReadSortKeyValues(ctx, row, partKeyValue, reverse, limit, offset)
	if err != nil || len(sortKeyValues) == 0 {
		return entities, nil
	}
	for _, sortKeyValue := range sortKeyValues {
		if entity, err := table.ReadEntityFromRow(ctx, row, partKeyValue, sortKeyValue); err == nil {
			// best effort; skip entities with broken JSON
			entities = append(entities, entity)
		}
	}
	return entities, nil
}

// ReadEntitiesFromRowAsJSON reads paginated entities from the specified row, returning a JSON byte slice.
func (table MemTable[T]) ReadEntitiesFromRowAsJSON(ctx context.Context, row TableRow[T], partKeyValue string, reverse bool, limit int, offset string) ([]byte, error) {
	if row.JsonValue == nil {
		return nil, errors.New("row " + row.RowName + " must contain compressed JSON values")
	}
	if partKeyValue == "" {
		return []byte("[]"), nil
	}
	sortKeyValues, err := table.ReadSortKeyValues(ctx, row, partKeyValue, reverse, limit, offset)
	if err != nil || len(sortKeyValues) == 0 {
		return []byte("[]"), nil
	}
	var i int
	var buf bytes.Buffer
	buf.WriteString("[")
	for _, sortKeyValue := range sortKeyValues {
		if jsonBytes, err := table.ReadEntityFromRowAsJSON(ctx, row, partKeyValue, sortKeyValue); err == nil {
			if i > 0 {
				buf.WriteString(",")
			}
			buf.Write(jsonBytes)
			i++
		}
	}
	buf.WriteString("]")
	return buf.Bytes(), nil
}

// ReadEntityRangeFromRow reads a range of entities from the specified row, where
// the sort key values range between the specified inclusive 'from' and 'to' values.
// If either 'from' or 'to' are empty, they will be replaced with the appropriate sentinel value.
func (table MemTable[T]) ReadEntityRangeFromRow(ctx context.Context, row TableRow[T], partKeyValue string, from, to string, reverse bool) ([]T, error) {
	entities := make([]T, 0)
	if row.JsonValue == nil {
		return entities, errors.New("row " + row.RowName + " must contain compressed JSON values")
	}
	if partKeyValue == "" {
		return entities, nil
	}
	sortKeyValues, err := table.ReadSortKeyValueRange(ctx, row, partKeyValue, from, to, reverse)
	if err != nil || len(sortKeyValues) == 0 {
		return entities, nil
	}
	for _, sortKeyValue := range sortKeyValues {
		if entity, err := table.ReadEntityFromRow(ctx, row, partKeyValue, sortKeyValue); err == nil {
			// best effort; skip entities with broken JSON
			entities = append(entities, entity)
		}
	}
	return entities, nil
}

// ReadEntityRangeFromRowAsJSON reads a range of entities from the specified row as JSON,
// where the sort key values range between the specified inclusive 'from' and 'to' values.
// If either 'from' or 'to' are empty, they will be replaced with the appropriate sentinel value.
func (table MemTable[T]) ReadEntityRangeFromRowAsJSON(ctx context.Context, row TableRow[T], partKeyValue string, from, to string, reverse bool) ([]byte, error) {
	if row.JsonValue == nil {
		return nil, errors.New("row " + row.RowName + " must contain compressed JSON values")
	}
	if partKeyValue == "" {
		return []byte("[]"), nil
	}
	sortKeyValues, err := table.ReadSortKeyValueRange(ctx, row, partKeyValue, from, to, reverse)
	if err != nil || len(sortKeyValues) == 0 {
		return []byte("[]"), nil
	}
	var i int
	var buf bytes.Buffer
	buf.WriteString("[")
	for _, sortKeyValue := range sortKeyValues {
		if jsonBytes, err := table.ReadEntityFromRowAsJSON(ctx, row, partKeyValue, sortKeyValue); err == nil {
			if i > 0 {
				buf.WriteString(",")
			}
			buf.Write(jsonBytes)
			i++
		}
	}
	buf.WriteString("]")
	return buf.Bytes(), nil
}

// ReadAllEntitiesFromRow reads all entities from the specified row.
// Note: this could be a lot of data! It should only be used for small collections.
func (table MemTable[T]) ReadAllEntitiesFromRow(ctx context.Context, row TableRow[T], partKeyValue string) ([]T, error) {
	entities := make([]T, 0)
	if row.JsonValue == nil {
		return entities, errors.New("row " + row.RowName + " must contain compressed JSON values")
	}
	if partKeyValue == "" {
		return entities, nil
	}
	sortKeyValues, err := table.ReadAllSortKeyValues(ctx, row, partKeyValue)
	if err != nil {
		return entities, fmt.Errorf("error reading all %s %s: %w",
			table.EntityType, row.getRowPartKeyValue(partKeyValue), err)
	}
	for _, sortKeyValue := range sortKeyValues {
		if entity, err := table.ReadEntityFromRow(ctx, row, partKeyValue, sortKeyValue); err == nil {
			// best effort; skip entities with broken JSON
			entities = append(entities, entity)
		}
	}
	return entities, nil
}

// ReadAllEntitiesFromRowAsJSON reads all entities from the specified row as a JSON byte slice.
// Note: this could be a lot of data! It should only be used for small collections.
func (table MemTable[T]) ReadAllEntitiesFromRowAsJSON(ctx context.Context, row TableRow[T], partKeyValue string) ([]byte, error) {
	if row.JsonValue == nil {
		return nil, errors.New("row " + row.RowName + " must contain compressed JSON values")
	}
	if partKeyValue == "" {
		return []byte("[]"), nil
	}
	sortKeyValues, err := table.ReadAllSortKeyValues(ctx, row, partKeyValue)
	if err != nil || len(sortKeyValues) == 0 {
		return []byte("[]"), nil
	}
	var i int
	var buf bytes.Buffer
	buf.WriteString("[")
	for _, sortKeyValue := range sortKeyValues {
		if jsonBytes, err := table.ReadEntityFromRowAsJSON(ctx, row, partKeyValue, sortKeyValue); err == nil {
			if i > 0 {
				buf.WriteString(",")
			}
			buf.Write(jsonBytes)
			i++
		}
	}
	buf.WriteString("]")
	return buf.Bytes(), nil
}

// ReadRecord reads the specified Record from the specified row.
func (table MemTable[T]) ReadRecord(ctx context.Context, row TableRow[T], partKeyValue string, sortKeyValue string) (Record, error) {
	r, ok := table.Records.GetRecord(row.getRowPartKeyValue(partKeyValue), sortKeyValue)
	if !ok {
		return Record{}, ErrNotFound
	}
	return r, nil
}

// ReadRecords reads paginated Records from the specified row.
func (table MemTable[T]) ReadRecords(ctx context.Context, row TableRow[T], partKeyValue string, reverse bool, limit int, offset string) ([]Record, error) {
	if partKeyValue == "" {
		return []Record{}, nil
	}
	sortKeyValues, err := table.ReadSortKeyValues(ctx, row, partKeyValue, reverse, limit, offset)
	if err != nil || len(sortKeyValues) == 0 {
		return []Record{}, err
	}
	return table.Records.GetRecords(row.getRowPartKeyValue(partKeyValue), sortKeyValues), nil
}

// ReadRecordRange reads a range of Records from the specified row,
// where the sort key values range between the specified inclusive 'from' and 'to' values.
// If either 'from' or 'to' are empty, they will be replaced with the appropriate sentinel value.
func (table MemTable[T]) ReadRecordRange(ctx context.Context, row TableRow[T], partKeyValue string, from, to string, reverse bool) ([]Record, error) {
	if partKeyValue == "" {
		return []Record{}, nil
	}
	sortKeyValues, err := table.ReadSortKeyValueRange(ctx, row, partKeyValue, from, to, reverse)
	if err != nil || len(sortKeyValues) == 0 {
		return []Record{}, err
	}
	return table.Records.GetRecords(row.getRowPartKeyValue(partKeyValue), sortKeyValues), nil
}

// ReadAllRecords reads all Records from the specified row.
// Note: this can return a very large number of values! Use with caution.
func (table MemTable[T]) ReadAllRecords(ctx context.Context, row TableRow[T], partKeyValue string) ([]Record, error) {
	if partKeyValue == "" {
		return []Record{}, nil
	}
	sortKeyValues, err := table.ReadAllSortKeyValues(ctx, row, partKeyValue)
	if err != nil || len(sortKeyValues) == 0 {
		return []Record{}, err
	}
	return table.Records.GetRecords(row.getRowPartKeyValue(partKeyValue), sortKeyValues), nil
}

// ReadEntityLabel reads the label for the specified entity.
func (table MemTable[T]) ReadEntityLabel(ctx context.Context, entityID string) (TextValue, error) {
	return table.ReadPartKeyLabel(ctx, table.EntityRow, entityID)
}

// ReadEntityLabels reads paginated entity labels.
func (table MemTable[T]) ReadEntityLabels(ctx context.Context, reverse bool, limit int, offset string) ([]TextValue, error) {
	return table.ReadPartKeyLabels(ctx, table.EntityRow, reverse, limit, offset)
}

// ReadEntityLabelRange reads a range of entity labels,
// where the sort key values range between the specified inclusive 'from' and 'to' values.
// If either 'from' or 'to' are empty, they will be replaced with the appropriate sentinel value.
func (table MemTable[T]) ReadEntityLabelRange(ctx context.Context, from, to string, reverse bool) ([]TextValue, error) {
	return table.ReadPartKeyLabelRange(ctx, table.EntityRow, from, to, reverse)
}

// ReadAllEntityLabels reads all entity labels.
// Note: this can return a very large number of values! Use with caution.
func (table MemTable[T]) ReadAllEntityLabels(ctx context.Context, sortByValue bool) ([]TextValue, error) {
	return table.ReadAllPartKeyLabels(ctx, table.EntityRow, sortByValue)
}

// FilterEntityLabels returns all entity labels that match the specified filter.
// The resulting text values are sorted by value.
func (table MemTable[T]) FilterEntityLabels(ctx context.Context, f func(TextValue) bool) ([]TextValue, error) {
	return table.FilterPartKeyLabels(ctx, table.EntityRow, f)
}

// ReadPartKeyLabel reads the text label for the specified partition key value from the specified row.
func (table MemTable[T]) ReadPartKeyLabel(ctx context.Context, row TableRow[T], partKeyValue string) (TextValue, error) {
	textValue := TextValue{Key: partKeyValue, Value: ""}
	if row.PartKeyLabel == nil {
		return textValue, errors.New("row " + row.RowName + " must contain partition key labels")
	}
	if partKeyValue == "" {
		return textValue, ErrNotFound
	}
	r, ok := table.Records.GetRecord(row.getRowPartKey(), partKeyValue)
	if !ok {
		return textValue, ErrNotFound
	}
	textValue.Key = r.SortKeyValue
	textValue.Value = r.TextValue
	return textValue, nil
}

// ReadPartKeyLabels reads paginated partition key labels from the specified row.
func (table MemTable[T]) ReadPartKeyLabels(ctx context.Context, row TableRow[T], reverse bool, limit int, offset string) ([]TextValue, error) {
	if row.PartKeyLabel == nil {
		return []TextValue{}, errors.New("row " + row.RowName + " must contain partition key labels")
	}
	sortKeyValues, err := table.ReadSortKeyValues(ctx, row, "", reverse, limit, offset)
	if err != nil || len(sortKeyValues) == 0 {
		return []TextValue{}, err
	}
	records := table.Records.GetRecords(row.getRowPartKey(), sortKeyValues)
	textValues := Map(records, func(r Record) TextValue {
		return TextValue{Key: r.SortKeyValue, Value: r.TextValue}
	})
	return textValues, nil
}

// ReadPartKeyLabelRange reads a range of partition key labels from the specified row,
// where the sort key values range between the specified inclusive 'from' and 'to' values.
// If either 'from' or 'to' are empty, they will be replaced with the appropriate sentinel value.
func (table MemTable[T]) ReadPartKeyLabelRange(ctx context.Context, row TableRow[T], from, to string, reverse bool) ([]TextValue, error) {
	if row.PartKeyLabel == nil {
		return []TextValue{}, errors.New("row " + row.RowName + " must contain partition key labels")
	}
	sortKeyValues, err := table.ReadSortKeyValueRange(ctx, row, "", from, to, reverse)
	if err != nil || len(sortKeyValues) == 0 {
		return []TextValue{}, err
	}
	records := table.Records.GetRecords(row.getRowPartKey(), sortKeyValues)
	textValues := Map(records, func(r Record) TextValue {
		return TextValue{Key: r.SortKeyValue, Value: r.TextValue}
	})
	return textValues, nil
}

// ReadAllPartKeyLabels reads all partition key labels from the specified row.
// Note: this can return a very large number of values! Use with caution.
func (table MemTable[T]) ReadAllPartKeyLabels(ctx context.Context, row TableRow[T], sortByValue bool) ([]TextValue, error) {
	if row.PartKeyLabel == nil {
		return []TextValue{}, errors.New("row " + row.RowName + " must contain partition key labels")
	}
	sortKeyValues, err := table.ReadAllSortKeyValues(ctx, row, "")
	if err != nil || len(sortKeyValues) == 0 {
		return []TextValue{}, err
	}
	records := table.Records.GetRecords(row.getRowPartKey(), sortKeyValues)
	textValues := Map(records, func(r Record) TextValue {
		return TextValue{Key: r.SortKeyValue, Value: r.TextValue}
	})
	if sortByValue {
		sort.Slice(textValues, func(i, j int) bool {
			return textValues[i].Value < textValues[j].Value
		})
	}
	return textValues, nil
}

// FilterPartKeyLabels returns all partition key labels from the specified row that match the specified filter.
// The resulting text values are sorted by value.
func (table MemTable[T]) FilterPartKeyLabels(ctx context.Context, row TableRow[T], f func(TextValue) bool) ([]TextValue, error) {
	if row.PartKeyLabel == nil {
		return []TextValue{}, errors.New("row " + row.RowName + " must contain partition key labels")
	}
	sortKeyValues, err := table.ReadAllSortKeyValues(ctx, row, "")
	if err != nil || len(sortKeyValues) == 0 {
		return []TextValue{}, err
	}
	records := table.Records.GetRecords(row.getRowPartKey(), sortKeyValues)
	textValues := Map(records, func(r Record) TextValue {
		return TextValue{Key: r.SortKeyValue, Value: r.TextValue}
	})
	textValues = Filter(textValues, f)
	sort.Slice(textValues, func(i, j int) bool {
		return textValues[i].Value < textValues[j].Value
	})
	return textValues, nil
}

// ReadTextValue reads the specified text value from the specified row.
func (table MemTable[T]) ReadTextValue(ctx context.Context, row TableRow[T], partKeyValue string, sortKeyValue string) (TextValue, error) {
	textValue := TextValue{Key: sortKeyValue, Value: ""}
	if row.TextValue == nil {
		return textValue, errors.New("row " + row.RowName + " must contain text values")
	}
	if partKeyValue == "" || sortKeyValue == "" {
		return textValue, ErrNotFound
	}
	r, ok := table.Records.GetRecord(row.getRowPartKeyValue(partKeyValue), sortKeyValue)
	if !ok {
		return textValue, ErrNotFound
	}
	textValue.Key = r.SortKeyValue
	textValue.Value = r.TextValue
	return textValue, nil
}

// ReadTextValues reads paginated text values from the specified row.
func (table MemTable[T]) ReadTextValues(ctx context.Context, row TableRow[T], partKeyValue string, reverse bool, limit int, offset string) ([]TextValue, error) {
	if row.TextValue == nil {
		return []TextValue{}, errors.New("row " + row.RowName + " must contain text values")
	}
	if partKeyValue == "" {
		return []TextValue{}, nil
	}
	sortKeyValues, err := table.ReadSortKeyValues(ctx, row, partKeyValue, reverse, limit, offset)
	if err != nil || len(sortKeyValues) == 0 {
		return []TextValue{}, err
	}
	records := table.Records.GetRecords(row.getRowPartKeyValue(partKeyValue), sortKeyValues)
	textValues := Map(records, func(r Record) TextValue {
		return TextValue{Key: r.SortKeyValue, Value: r.TextValue}
	})
	return textValues, nil
}

// ReadTextValueRange reads a range of text values from the specified row,
// where the sort key values range between the specified inclusive 'from' and 'to' values.
// If either 'from' or 'to' are empty, they will be replaced with the appropriate sentinel value.
func (table MemTable[T]) ReadTextValueRange(ctx context.Context, row TableRow[T], partKeyValue string, from, to string, reverse bool) ([]TextValue, error) {
	if row.TextValue == nil {
		return []TextValue{}, errors.New("row " + row.RowName + " must contain text values")
	}
	if partKeyValue == "" {
		return []TextValue{}, nil
	}
	sortKeyValues, err := table.ReadSortKeyValueRange(ctx, row, partKeyValue, from, to, reverse)
	if err != nil || len(sortKeyValues) == 0 {
		return []TextValue{}, err
	}
	records := table.Records.GetRecords(row.getRowPartKeyValue(partKeyValue), sortKeyValues)
	textValues := Map(records, func(r Record) TextValue {
		return TextValue{Key: r.SortKeyValue, Value: r.TextValue}
	})
	return textValues, nil
}

// ReadAllTextValues reads all text values from the specified row.
// Note: this can return a very large number of values! Use with caution.
func (table MemTable[T]) ReadAllTextValues(ctx context.Context, row TableRow[T], partKeyValue string, sortByValue bool) ([]TextValue, error) {
	if row.TextValue == nil {
		return []TextValue{}, errors.New("row " + row.RowName + " must contain text values")
	}
	if partKeyValue == "" {
		return []TextValue{}, nil
	}
	sortKeyValues, err := table.ReadAllSortKeyValues(ctx, row, partKeyValue)
	if err != nil || len(sortKeyValues) == 0 {
		return []TextValue{}, err
	}
	records := table.Records.GetRecords(row.getRowPartKeyValue(partKeyValue), sortKeyValues)
	textValues := Map(records, func(r Record) TextValue {
		return TextValue{Key: r.SortKeyValue, Value: r.TextValue}
	})
	if sortByValue {
		sort.Slice(textValues, func(i, j int) bool {
			return textValues[i].Value < textValues[j].Value
		})
	}
	return textValues, nil
}

// FilterTextValues returns all text values from the specified row that match the specified filter.
// The resulting text values are sorted by value.
func (table MemTable[T]) FilterTextValues(ctx context.Context, row TableRow[T], partKeyValue string, f func(TextValue) bool) ([]TextValue, error) {
	if row.TextValue == nil {
		return []TextValue{}, errors.New("row " + row.RowName + " must contain text values")
	}
	if partKeyValue == "" {
		return []TextValue{}, nil
	}
	sortKeyValues, err := table.ReadAllSortKeyValues(ctx, row, partKeyValue)
	if err != nil || len(sortKeyValues) == 0 {
		return []TextValue{}, err
	}
	records := table.Records.GetRecords(row.getRowPartKeyValue(partKeyValue), sortKeyValues)
	textValues := Map(records, func(r Record) TextValue {
		return TextValue{Key: r.SortKeyValue, Value: r.TextValue}
	})
	textValues = Filter(textValues, f)
	sort.Slice(textValues, func(i, j int) bool {
		return textValues[i].Value < textValues[j].Value
	})
	return textValues, nil
}

// ReadNumericValue reads the specified numeric value from the specified row.
func (table MemTable[T]) ReadNumericValue(ctx context.Context, row TableRow[T], partKeyValue string, sortKeyValue string) (NumValue, error) {
	numValue := NumValue{Key: sortKeyValue, Value: 0}
	if row.NumericValue == nil {
		return numValue, errors.New("row " + row.RowName + " must contain numeric values")
	}
	if partKeyValue == "" || sortKeyValue == "" {
		return numValue, ErrNotFound
	}
	r, ok := table.Records.GetRecord(row.getRowPartKeyValue(partKeyValue), sortKeyValue)
	if !ok {
		return numValue, ErrNotFound
	}
	numValue.Key = r.SortKeyValue
	numValue.Value = r.NumericValue
	return numValue, nil
}

// ReadNumericValues reads paginated numeric values from the specified row.
func (table MemTable[T]) ReadNumericValues(ctx context.Context, row TableRow[T], partKeyValue string, reverse bool, limit int, offset string) ([]NumValue, error) {
	if row.NumericValue == nil {
		return []NumValue{}, errors.New("row " + row.RowName + " must contain numeric values")
	}
	if partKeyValue == "" {
		return []NumValue{}, nil
	}
	sortKeyValues, err := table.ReadSortKeyValues(ctx, row, partKeyValue, reverse, limit, offset)
	if err != nil || len(sortKeyValues) == 0 {
		return []NumValue{}, err
	}
	records := table.Records.GetRecords(row.getRowPartKeyValue(partKeyValue), sortKeyValues)
	numValues := Map(records, func(r Record) NumValue {
		return NumValue{Key: r.SortKeyValue, Value: r.NumericValue}
	})
	return numValues, nil
}

// ReadNumericValueRange reads a range of numeric values from the specified row,
// where the sort key values range between the specified inclusive 'from' and 'to' values.
// If either 'from' or 'to' are empty, they will be replaced with the appropriate sentinel value.
func (table MemTable[T]) ReadNumericValueRange(ctx context.Context, row TableRow[T], partKeyValue string, from, to string, reverse bool) ([]NumValue, error) {
	if row.NumericValue == nil {
		return []NumValue{}, errors.New("row " + row.RowName + " must contain numeric values")
	}
	if partKeyValue == "" {
		return []NumValue{}, nil
	}
	sortKeyValues, err := table.ReadSortKeyValueRange(ctx, row, partKeyValue, from, to, reverse)
	if err != nil || len(sortKeyValues) == 0 {
		return []NumValue{}, err
	}
	records := table.Records.GetRecords(row.getRowPartKeyValue(partKeyValue), sortKeyValues)
	numValues := Map(records, func(r Record) NumValue {
		return NumValue{Key: r.SortKeyValue, Value: r.NumericValue}
	})
	return numValues, nil
}

// ReadAllNumericValues reads all numeric values from the specified row.
// Note: this can return a very large number of values! Use with caution.
func (table MemTable[T]) ReadAllNumericValues(ctx context.Context, row TableRow[T], partKeyValue string, sortByValue bool) ([]NumValue, error) {
	if row.NumericValue == nil {
		return []NumValue{}, errors.New("row " + row.RowName + " must contain numeric values")
	}
	if partKeyValue == "" {
		return []NumValue{}, nil
	}
	sortKeyValues, err := table.ReadAllSortKeyValues(ctx, row, partKeyValue)
	if err != nil || len(sortKeyValues) == 0 {
		return []NumValue{}, err
	}
	records := table.Records.GetRecords(row.getRowPartKeyValue(partKeyValue), sortKeyValues)
	numValues := Map(records, func(r Record) NumValue {
		return NumValue{Key: r.SortKeyValue, Value: r.NumericValue}
	})
	if sortByValue {
		sort.Slice(numValues, func(i, j int) bool {
			return numValues[i].Value < numValues[j].Value
		})
	}
	return numValues, nil
}

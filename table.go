package versionary

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
)

// ErrNotFound is returned when a specified thing is not found
var ErrNotFound = errors.New("versionary: not found")

// TableWriter is the interface that defines methods for writing, updating, or deleting an entity from a DynamoDB table
// based on an opinionated implementation of DynamoDB by Table.
type TableWriter[T any] interface {
	WriteEntity(ctx context.Context, entity T) error
	UpdateEntity(ctx context.Context, entity T) error
	UpdateEntityVersion(ctx context.Context, oldVersion T, newVersion T) error
	DeleteEntity(ctx context.Context, entity T) error
	DeleteEntityWithID(ctx context.Context, entityID string) (T, error)
	DeleteEntityVersionWithID(ctx context.Context, entityID, versionID string) (T, error)
}

// TableReader is the interface that defines methods for reading entity-related information from a DynamoDB table based
// on an opinionated implementation of DynamoDB by Table.
type TableReader[T any] interface {
	IsValid() bool
	GetEntityType() string
	GetTableName() string
	GetRow(rowName string) (TableRow[T], bool)
	GetEntityRow() TableRow[T]
	EntityID(entity T) string
	EntityVersionID(entity T) string
	EntityReferenceID(entity T) string
	EntityExists(ctx context.Context, entityID string) bool
	EntityVersionExists(ctx context.Context, entityID string, versionID string) bool
	CountPartKeyValues(ctx context.Context, row TableRow[T]) (int64, error)
	CountSortKeyValues(ctx context.Context, row TableRow[T], partKeyValue string) (int64, error)
	ReadAllPartKeyValues(ctx context.Context, row TableRow[T]) ([]string, error)
	ReadAllSortKeyValues(ctx context.Context, row TableRow[T], partKeyValue string) ([]string, error)
	ReadPartKeyValues(ctx context.Context, row TableRow[T], reverse bool, limit int, offset string) ([]string, error)
	ReadSortKeyValues(ctx context.Context, row TableRow[T], partKeyValue string, reverse bool, limit int, offset string) ([]string, error)
	ReadFirstSortKeyValue(ctx context.Context, row TableRow[T], partKeyValue string) (string, error)
	ReadLastSortKeyValue(ctx context.Context, row TableRow[T], partKeyValue string) (string, error)
	ReadAllEntityIDs(ctx context.Context) ([]string, error)
	ReadEntityIDs(ctx context.Context, reverse bool, limit int, offset string) ([]string, error)
	ReadAllEntityVersionIDs(ctx context.Context, entityID string) ([]string, error)
	ReadEntityVersionIDs(ctx context.Context, entityID string, reverse bool, limit int, offset string) ([]string, error)
	ReadCurrentEntityVersionID(ctx context.Context, entityID string) (string, error)
	ReadEntities(ctx context.Context, entityIDs []string) []T
	ReadEntitiesAsJSON(ctx context.Context, entityIDs []string) []byte
	ReadEntity(ctx context.Context, entityID string) (T, error)
	ReadEntityAsJSON(ctx context.Context, entityID string) ([]byte, error)
	ReadEntityAsCompressedJSON(ctx context.Context, entityID string) ([]byte, error)
	ReadEntityVersion(ctx context.Context, entityID string, versionID string) (T, error)
	ReadEntityVersionAsJSON(ctx context.Context, entityID string, versionID string) ([]byte, error)
	ReadEntityVersions(ctx context.Context, entityID string, reverse bool, limit int, offset string) ([]T, error)
	ReadEntityVersionsAsJSON(ctx context.Context, entityID string, reverse bool, limit int, offset string) ([]byte, error)
	ReadAllEntityVersions(ctx context.Context, entityID string) ([]T, error)
	ReadAllEntityVersionsAsJSON(ctx context.Context, entityID string) ([]byte, error)
	ReadEntityFromRow(ctx context.Context, row TableRow[T], partKeyValue string, sortKeyValue string) (T, error)
	ReadEntityFromRowAsJSON(ctx context.Context, row TableRow[T], partKeyValue string, sortKeyValue string) ([]byte, error)
	ReadEntityFromRowAsCompressedJSON(ctx context.Context, row TableRow[T], partKeyValue string, sortKeyValue string) ([]byte, error)
	ReadEntitiesFromRow(ctx context.Context, row TableRow[T], partKeyValue string, reverse bool, limit int, offset string) ([]T, error)
	ReadEntitiesFromRowAsJSON(ctx context.Context, row TableRow[T], partKeyValue string, reverse bool, limit int, offset string) ([]byte, error)
	ReadAllEntitiesFromRow(ctx context.Context, row TableRow[T], partKeyValue string) ([]T, error)
	ReadAllEntitiesFromRowAsJSON(ctx context.Context, row TableRow[T], partKeyValue string) ([]byte, error)
	ReadRecord(ctx context.Context, row TableRow[T], partKeyValue string, sortKeyValue string) (Record, error)
	ReadRecords(ctx context.Context, row TableRow[T], partKeyValue string, reverse bool, limit int, offset string) ([]Record, error)
	ReadAllRecords(ctx context.Context, row TableRow[T], partKeyValue string) ([]Record, error)
	ReadTextValue(ctx context.Context, row TableRow[T], partKeyValue string, sortKeyValue string) (TextValue, error)
	ReadTextValues(ctx context.Context, row TableRow[T], partKeyValue string, reverse bool, limit int, offset string) ([]TextValue, error)
	ReadAllTextValues(ctx context.Context, row TableRow[T], partKeyValue string, sortByValue bool) ([]TextValue, error)
	FilterTextValues(ctx context.Context, row TableRow[T], partKeyValue string, f func(TextValue) bool) ([]TextValue, error)
	ReadNumericValue(ctx context.Context, row TableRow[T], partKeyValue string, sortKeyValue string) (NumValue, error)
	ReadNumericValues(ctx context.Context, row TableRow[T], partKeyValue string, reverse bool, limit int, offset string) ([]NumValue, error)
	ReadAllNumericValues(ctx context.Context, row TableRow[T], partKeyValue string, sortByValue bool) ([]NumValue, error)
}

// TableReadWriter is both a TableReader and TableWriter
type TableReadWriter[T any] interface {
	TableReader[T]
	TableWriter[T]
}

// TableRow represents a single "wide row" in an Entity Table. A "wide row" is used to quickly provide a response to a
// specific query, or question for the data. Example: "What error events occurred on this day?"
//
// The following attributes are stored in the table. These attribute names are configurable.
//   - v_part: partition key (required)
//   - v_sort: sorting key (required; for an entity row without versions, reuse the partition key)
//   - v_json: gzip compressed JSON data (optional)
//   - v_text: string value (optional)
//   - v_num: double-wide floating point numeric value (optional)
//   - v_expires: expiration timestamp in epoch seconds (optional)
//
// Pipe-separated key values are used to avoid naming collisions, supporting multiple row types in a single table:
// Entity Values: rowName|partKeyName|partKeyValue -- (sortKeyValue, value), (sortKeyValue, value), ...
// Partition Keys: rowName|partKeyName -- partKeyValue, partKeyValue, ...
// Note that in addition to storing the entity data in a given row, we're also storing the partition key values used
// for each row, to support queries that "walk" the entire data set for a given row type.
type TableRow[T any] struct {
	RowName       string
	PartKeyName   string
	PartKeyValue  func(T) string
	PartKeyValues func(T) []string
	SortKeyName   string
	SortKeyValue  func(T) string
	JsonValue     func(T) []byte
	TextValue     func(T) string
	NumericValue  func(T) float64
	TimeToLive    func(T) int64
}

// IsValid returns true if the TableRow contains all required fields.
func (row TableRow[T]) IsValid() bool {
	return row.RowName != "" && row.PartKeyName != "" && row.SortKeyName != "" &&
		!(row.PartKeyValue == nil && row.PartKeyValues == nil) && row.SortKeyValue != nil
}

// getRowPartKey generates a pipe-delimited partition key used for storing partition key values for this row
func (row TableRow[T]) getRowPartKey() string {
	return row.RowName + "|" + row.PartKeyName
}

// getRowPartKeyValue generates a pipe-delimited partition key used for storing entity values in this row
func (row TableRow[T]) getRowPartKeyValue(value string) string {
	return row.RowName + "|" + row.PartKeyName + "|" + value
}

// getPartKeyValues extracts partition key values from the provided entity, filtering out any blank strings.
// Note that these values are not prefixed with the row partition key.
func (row TableRow[T]) getPartKeyValues(entity T) []string {
	var keys []string
	if row.PartKeyValue != nil {
		value := row.PartKeyValue(entity)
		if value != "" {
			keys = append(keys, value)
		}
	}
	if row.PartKeyValues != nil {
		for _, value := range row.PartKeyValues(entity) {
			if value != "" {
				keys = append(keys, value)
			}
		}
	}
	return keys
}

// getDeletedPartKeyValues identifies partition key values that were removed from the new version of an entity.
// This helps us identify index keys that need to be deleted from "index" rows.
func (row TableRow[T]) getDeletedPartKeyValues(oldVersion, newVersion T) []string {
	oldKeys := row.getPartKeyValues(oldVersion)
	newKeys := row.getPartKeyValues(newVersion)
	if (len(oldKeys) == 0) || (len(newKeys) == 0) {
		return oldKeys
	}
	return Filter(oldKeys, func(key string) bool { return !Contains(newKeys, key) })
}

// getAddedPartKeyValues identifies partition key values that were added to the new version of an entity.
// This helps us identify index keys that need to be added to "index" rows.
func (row TableRow[T]) getAddedPartKeyValues(oldVersion, newVersion T) []string {
	oldKeys := row.getPartKeyValues(oldVersion)
	newKeys := row.getPartKeyValues(newVersion)
	if (len(oldKeys) == 0) || (len(newKeys) == 0) {
		return newKeys
	}
	return Filter(newKeys, func(key string) bool { return !Contains(oldKeys, key) })
}

// getWriteRecords generates a list of new Records for persisting the provided entity.
func (row TableRow[T]) getWriteRecords(entity T) []Record {
	var records []Record
	// Validate the partition and sorting keys
	sortKey := row.SortKeyValue(entity)
	partKeyValues := row.getPartKeyValues(entity)
	if len(partKeyValues) == 0 || sortKey == "" {
		return records
	}

	// Generate records for each partition key
	var jsonValue []byte // cache the compressed JSON value for the entity
	for _, partKeyValue := range partKeyValues {
		// Generate a Record for this partition key
		// Values: rowName|partKeyName|partKeyValue -- (sortKeyValue, value), (sortKeyValue, value), ...
		r := Record{
			PartKeyValue: row.getRowPartKeyValue(partKeyValue), // rowName|partKeyName|partKeyValue
			SortKeyValue: sortKey,
		}
		if row.JsonValue != nil {
			if jsonValue == nil {
				jsonValue = row.JsonValue(entity)
			}
			r.JsonValue = jsonValue
		}
		if row.TextValue != nil {
			r.TextValue = row.TextValue(entity)
		}
		if row.NumericValue != nil {
			r.NumericValue = row.NumericValue(entity)
		}
		if row.TimeToLive != nil {
			r.TimeToLive = row.TimeToLive(entity)
		}
		records = append(records, r)

		// Track the unique partition key values for this row
		// Partition Keys: rowName|partKeyName -- partKeyValue, partKeyValue, ...
		k := Record{
			PartKeyValue: row.getRowPartKey(), // rowName|partKeyName
			SortKeyValue: partKeyValue,
		}
		if row.TimeToLive != nil {
			k.TimeToLive = row.TimeToLive(entity)
		}
		records = append(records, k)
	}
	return records
}

// getDeleteRecords generates a list Records for deleting the specified entity.
func (row TableRow[T]) getDeleteRecords(entity T) []Record {
	return row.getDeleteRecordsForKeys(row.getPartKeyValues(entity), row.SortKeyValue(entity))
}

// getDeleteRecordsForKeys generates a list of delete Records for the specified partition and sorting keys.
// Note that we're not deleting the partition key values from the row that tracks the partition key values used,
// because other entities may use the same partition key values. Clearing obsolete partition key values from that
// row would require an expensive "walk the list" maintenance operation; it can be done if it's needed.
func (row TableRow[T]) getDeleteRecordsForKeys(partKeyValues []string, sortKeyValue string) []Record {
	var records []Record
	// Validate the partition and sorting keys
	if len(partKeyValues) == 0 || sortKeyValue == "" {
		return records
	}
	// Generate delete records for each partition key
	for _, partKeyValue := range partKeyValues {
		// Generate a record for this partition key
		// Values: rowName|partKeyName|partKeyValue -- (sortKeyValue, value), (sortKeyValue, value), ...
		r := Record{
			PartKeyValue: row.getRowPartKeyValue(partKeyValue), // rowName|partKeyName|partKeyValue
			SortKeyValue: sortKeyValue,
		}
		records = append(records, r)
	}
	return records
}

// Table represents a single DynamoDB table that stores all the "wide rows" for a given entity. Denormalized
// entity data are stored in a single table, reusing the attribute names indicated in the Table definition.
// The EntityRow is special; it contains the revision history for the entity, whereas the IndexRows contain only
// values from the latest revision of the entity.
type Table[T any] struct {
	Client           *dynamodb.Client       // AWS DynamoDB client
	EntityType       string                 // the type of entity stored in this table
	TableName        string                 // name of the table, typically including the entity type and environment name
	PartKeyAttr      string                 // partition key attribute name (e.g. "v_part")
	SortKeyAttr      string                 // sort key attribute name (e.g. "v_sort")
	JsonValueAttr    string                 // JSON value attribute name (e.g. "v_json")
	TextValueAttr    string                 // text value attribute name (e.g. "v_text")
	NumericValueAttr string                 // numeric value attribute name (e.g. "v_num")
	TimeToLiveAttr   string                 // time to live attribute name (e.g. "v_expires")
	TTL              bool                   // true if the table has a time to live attribute
	EntityRow        TableRow[T]            // the row that stores the entity versions
	IndexRows        map[string]TableRow[T] // index rows, based on various entity properties
}

// IsValid returns true if the table fields and rows are valid.
func (table Table[T]) IsValid() bool {
	for _, row := range table.IndexRows {
		if !row.IsValid() {
			return false
		}
	}
	return table.Client != nil && table.EntityType != "" && table.TableName != "" && table.EntityRow.IsValid()
}

// itemToRecord converts a DynamoDB AttributeValue map (an "item") to a Record.
func (table Table[T]) itemToRecord(item map[string]types.AttributeValue) Record {
	r := Record{}
	if item == nil {
		return r
	}
	if v, ok := item[table.getPartKeyAttr()]; ok {
		r.PartKeyValue = v.(*types.AttributeValueMemberS).Value
	}
	if v, ok := item[table.getSortKeyAttr()]; ok {
		r.SortKeyValue = v.(*types.AttributeValueMemberS).Value
	}
	if v, ok := item[table.getJsonValueAttr()]; ok {
		r.JsonValue = v.(*types.AttributeValueMemberB).Value
	}
	if v, ok := item[table.getTextValueAttr()]; ok {
		r.TextValue = v.(*types.AttributeValueMemberS).Value
	}
	if v, ok := item[table.getNumericValueAttr()]; ok {
		r.NumericValue, _ = strconv.ParseFloat(v.(*types.AttributeValueMemberN).Value, 64)
	}
	if v, ok := item[table.getTimeToLiveAttr()]; ok {
		r.TimeToLive, _ = strconv.ParseInt(v.(*types.AttributeValueMemberN).Value, 10, 64)
	}
	return r
}

// putRequest converts a Record to a DynamoDB WriteRequest containing a PutRequest.
func (table Table[T]) putRequest(r Record) types.WriteRequest {
	item := map[string]types.AttributeValue{}
	if r.PartKeyValue != "" {
		item[table.getPartKeyAttr()] = &types.AttributeValueMemberS{Value: r.PartKeyValue}
	}
	if r.SortKeyValue != "" {
		item[table.getSortKeyAttr()] = &types.AttributeValueMemberS{Value: r.SortKeyValue}
	}
	if r.JsonValue != nil {
		item[table.getJsonValueAttr()] = &types.AttributeValueMemberB{Value: r.JsonValue}
	}
	if r.TextValue != "" {
		item[table.getTextValueAttr()] = &types.AttributeValueMemberS{Value: r.TextValue}
	}
	if r.NumericValue != 0 {
		item[table.getNumericValueAttr()] = &types.AttributeValueMemberN{Value: strconv.FormatFloat(r.NumericValue, 'f', -1, 64)}
	}
	if r.TimeToLive != 0 {
		item[table.getTimeToLiveAttr()] = &types.AttributeValueMemberN{Value: strconv.FormatInt(r.TimeToLive, 10)}
	}
	return types.WriteRequest{PutRequest: &types.PutRequest{Item: item}}
}

// deleteRequest converts a Record to a DynamoDB WriteRequest containing a DeleteRequest.
func (table Table[T]) deleteRequest(r Record) types.WriteRequest {
	key := map[string]types.AttributeValue{}
	if r.PartKeyValue != "" {
		key[table.getPartKeyAttr()] = &types.AttributeValueMemberS{Value: r.PartKeyValue}
	}
	if r.SortKeyValue != "" {
		key[table.getSortKeyAttr()] = &types.AttributeValueMemberS{Value: r.SortKeyValue}
	}
	return types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: key}}
}

// getPartKeyAttr returns the configured partition key attribute name. The default is "part_key".
func (table Table[T]) getPartKeyAttr() string {
	if table.PartKeyAttr == "" {
		return "part_key"
	}
	return table.PartKeyAttr
}

// getSortKeyAttr returns the configured sort key attribute name. The default is "sort_key".
func (table Table[T]) getSortKeyAttr() string {
	if table.SortKeyAttr == "" {
		return "sort_key"
	}
	return table.SortKeyAttr
}

// getJsonValueAttr returns the configured JSON value attribute name. The default is "json_value".
func (table Table[T]) getJsonValueAttr() string {
	if table.JsonValueAttr == "" {
		return "json_value"
	}
	return table.JsonValueAttr
}

// getTextValueAttr returns the configured text value attribute name. The default is "text_value".
func (table Table[T]) getTextValueAttr() string {
	if table.TextValueAttr == "" {
		return "text_value"
	}
	return table.TextValueAttr
}

// getNumericValueAttr returns the configured numeric value attribute name. The default is "num_value".
func (table Table[T]) getNumericValueAttr() string {
	if table.NumericValueAttr == "" {
		return "num_value"
	}
	return table.NumericValueAttr
}

// getTimeToLiveAttr returns the configured time to live attribute name. The default is "expires_at".
func (table Table[T]) getTimeToLiveAttr() string {
	if table.TimeToLiveAttr == "" {
		return "expires_at"
	}
	return table.TimeToLiveAttr
}

// getAttributeNames returns a slice of all configured attribute names.
func (table Table[T]) getAttributeNames() []string {
	return []string{
		table.getPartKeyAttr(),
		table.getSortKeyAttr(),
		table.getJsonValueAttr(),
		table.getTextValueAttr(),
		table.getNumericValueAttr(),
		table.getTimeToLiveAttr(),
	}
}

// GetEntityType returns the name of the entity type stored in this table.
func (table Table[T]) GetEntityType() string {
	return table.EntityType
}

// GetTableName returns the name of the table.
func (table Table[T]) GetTableName() string {
	return table.TableName
}

// GetRow returns the specified row definition.
func (table Table[T]) GetRow(rowName string) (TableRow[T], bool) {
	if rowName == table.EntityRow.RowName {
		return table.EntityRow, true
	}
	row, ok := table.IndexRows[rowName]
	return row, ok
}

// GetEntityRow returns the entity row definition. This row contains the revision history for the entity.
func (table Table[T]) GetEntityRow() TableRow[T] {
	return table.EntityRow
}

// GetTableDescription fetches metadata about the DynamoDB table.
func (table Table[T]) GetTableDescription(ctx context.Context) (*types.TableDescription, error) {
	res, err := table.Client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(table.TableName),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to describe table %s: %w", table.TableName, err)
	}
	return res.Table, nil
}

// TableExists returns true if the DynamoDB table exists.
func (table Table[T]) TableExists(ctx context.Context) bool {
	desc, err := table.GetTableDescription(ctx)
	if err != nil {
		// Table Not Found is an expected error
		var nf *types.ResourceNotFoundException
		if errors.As(err, &nf) {
			log.Println("table", table.TableName, nf.ErrorCode())
			return false
		}
		// Identify other possible errors
		log.Println(err.Error())
		var oe *smithy.OperationError
		if errors.As(err, &oe) {
			log.Printf("%s operation %s error: %v\n", oe.Service(), oe.Operation(), oe.Unwrap())
		}
		var ae smithy.APIError
		if errors.As(err, &ae) {
			log.Printf("%s: %s (fault: %s)\n", ae.ErrorCode(), ae.ErrorMessage(), ae.ErrorFault().String())
		}
		return false
	}
	log.Println("table", *desc.TableName, desc.TableStatus)
	return true
}

// CreateTable creates the DynamoDB table for the entity.
// This operation may take a few seconds to complete, as it waits for the table to become active.
func (table Table[T]) CreateTable(ctx context.Context) error {
	startTime := time.Now()
	partKeyAttr := types.AttributeDefinition{
		AttributeName: aws.String(table.getPartKeyAttr()),
		AttributeType: types.ScalarAttributeTypeS,
	}
	sortKeyAttr := types.AttributeDefinition{
		AttributeName: aws.String(table.getSortKeyAttr()),
		AttributeType: types.ScalarAttributeTypeS,
	}
	partKeyDef := types.KeySchemaElement{
		AttributeName: aws.String(table.getPartKeyAttr()),
		KeyType:       types.KeyTypeHash,
	}
	sortKeyDef := types.KeySchemaElement{
		AttributeName: aws.String(table.getSortKeyAttr()),
		KeyType:       types.KeyTypeRange,
	}
	req := dynamodb.CreateTableInput{
		TableName:            aws.String(table.TableName),
		TableClass:           types.TableClassStandard,
		BillingMode:          types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{partKeyAttr, sortKeyAttr},
		KeySchema:            []types.KeySchemaElement{partKeyDef, sortKeyDef},
	}
	create, err := table.Client.CreateTable(ctx, &req)
	if err != nil {
		return fmt.Errorf("failed to create table %s: %w", table.TableName, err)
	}
	t := create.TableDescription
	log.Println("table", *t.TableName, t.TableStatus, time.Since(startTime))

	// Use a Waiter to detect the transition from CREATING to ACTIVE:
	// https://aws.github.io/aws-sdk-go-v2/docs/making-requests/#using-waiters
	waiter := dynamodb.NewTableExistsWaiter(table.Client, func(o *dynamodb.TableExistsWaiterOptions) {
		o.MinDelay = 3 * time.Second   // default is 20 seconds
		o.MaxDelay = 120 * time.Second // default is 120 seconds
	})
	describe, err := waiter.WaitForOutput(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(table.TableName)}, 3*time.Minute)
	if err != nil {
		return fmt.Errorf("failed waiting for table %s to become active: %w", table.TableName, err)
	}
	t = describe.Table
	log.Println("table", *t.TableName, t.TableStatus, time.Since(startTime))

	// Update the TTL on the table if requested
	if table.TTL {
		err = table.UpdateTTL(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

// UpdateTTL updates the DynamoDB table's time-to-live settings.
func (table Table[T]) UpdateTTL(ctx context.Context) error {
	update, err := table.Client.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String(table.TableName),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			AttributeName: aws.String(table.getTimeToLiveAttr()),
			Enabled:       aws.Bool(table.TTL),
		},
	})
	if err != nil {
		return fmt.Errorf("failed to update TTL on table %s: %w", table.TableName, err)
	}
	t := update.TimeToLiveSpecification
	log.Println("table", table.TableName, "TTL", *t.AttributeName, *t.Enabled)
	return nil
}

// DeleteTable deletes the DynamoDB table. Note that the table's status may be DELETING for a few seconds.
func (table Table[T]) DeleteTable(ctx context.Context) error {
	res, err := table.Client.DeleteTable(ctx, &dynamodb.DeleteTableInput{
		TableName: aws.String(table.TableName),
	})
	if err != nil {
		return fmt.Errorf("failed to delete table %s: %w", table.TableName, err)
	}
	t := res.TableDescription
	log.Println("table", *t.TableName, t.TableStatus)
	return nil
}

// EntityID returns the unique entity ID for the provided entity.
func (table Table[T]) EntityID(entity T) string {
	if table.EntityRow.PartKeyValue == nil {
		return ""
	}
	return table.EntityRow.PartKeyValue(entity)
}

// EntityVersionID returns the unique entity version ID for the provided entity.
func (table Table[T]) EntityVersionID(entity T) string {
	if table.EntityRow.SortKeyValue == nil {
		return ""
	}
	return table.EntityRow.SortKeyValue(entity)
}

// EntityReferenceID returns a unique Reference ID (EntityType-ID-VersionID) for the provided entity.
func (table Table[T]) EntityReferenceID(entity T) string {
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
func (table Table[T]) EntityExists(ctx context.Context, entityID string) bool {
	versionID, err := table.ReadFirstSortKeyValue(ctx, table.EntityRow, entityID)
	return err == nil && versionID != ""
}

// EntityVersionExists checks if an entity version exists in the table.
func (table Table[T]) EntityVersionExists(ctx context.Context, entityID string, versionID string) bool {
	if entityID == "" || versionID == "" {
		return false
	}
	req := dynamodb.GetItemInput{
		TableName: aws.String(table.TableName),
		Key: map[string]types.AttributeValue{
			table.getPartKeyAttr(): &types.AttributeValueMemberS{Value: table.EntityRow.getRowPartKeyValue(entityID)},
			table.getSortKeyAttr(): &types.AttributeValueMemberS{Value: versionID},
		},
		ProjectionExpression: aws.String(table.getPartKeyAttr() + ", " + table.getSortKeyAttr()),
	}
	res, err := table.Client.GetItem(ctx, &req)
	return err == nil && res.Item != nil && res.Item[table.getPartKeyAttr()] != nil && res.Item[table.getSortKeyAttr()] != nil
}

// writeBatches bulk-writes the provided WriteRequests to the table, respecting DynamoDB limits:
// 25 write requests per batch, 16MB per batch, and 400KB per item. For more information, see:
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html
func (table Table[T]) writeBatches(ctx context.Context, writeRequests []types.WriteRequest) error {
	batches := Batch(writeRequests, 25)
	for _, batch := range batches {
		requestItems := map[string][]types.WriteRequest{
			table.TableName: batch,
		}
		for len(requestItems) > 0 {
			// Create and send a batch write request
			req := dynamodb.BatchWriteItemInput{RequestItems: requestItems}
			res, err := table.Client.BatchWriteItem(ctx, &req)
			if err != nil {
				return fmt.Errorf("failed writing batch to table %s: %w", table.TableName, err)
			}
			// Check the response for any unprocessed items and try again
			if len(res.UnprocessedItems) > 0 {
				requestItems = res.UnprocessedItems
			} else {
				requestItems = nil
			}
		}
	}
	return nil
}

// WriteEntity batch-writes the provided entity to each of the wide rows in the table.
func (table Table[T]) WriteEntity(ctx context.Context, entity T) error {
	records := table.EntityRow.getWriteRecords(entity)
	if table.IndexRows != nil {
		for _, row := range table.IndexRows {
			records = append(records, row.getWriteRecords(entity)...)
		}
	}
	writeRequests := Map(records, func(r Record) types.WriteRequest { return table.putRequest(r) })
	err := table.writeBatches(ctx, writeRequests)
	if err != nil {
		return fmt.Errorf("failed writing %s: %w", table.EntityReferenceID(entity), err)
	}
	return nil
}

// UpdateEntity batch-writes the provided entity to each of the wide rows in the table,
// including deleting any obsolete values in index rows. If a previous version does not exist,
// it is treated as a new entity.
func (table Table[T]) UpdateEntity(ctx context.Context, entity T) error {
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
func (table Table[T]) UpdateEntityVersion(ctx context.Context, oldVersion T, newVersion T) error {
	// Generate deletion requests for deleted index row partition keys
	var deleteRecords []Record
	for _, row := range table.IndexRows {
		deletedKeys := row.getDeletedPartKeyValues(oldVersion, newVersion)
		sortKeyValue := row.SortKeyValue(oldVersion)
		deleteRecords = append(deleteRecords, row.getDeleteRecordsForKeys(deletedKeys, sortKeyValue)...)
	}
	writeRequests := Map(deleteRecords, func(r Record) types.WriteRequest { return table.deleteRequest(r) })
	// Generate put requests for the new version of the entity
	writeRecords := table.EntityRow.getWriteRecords(newVersion)
	for _, row := range table.IndexRows {
		writeRecords = append(writeRecords, row.getWriteRecords(newVersion)...)
	}
	for _, record := range writeRecords {
		writeRequests = append(writeRequests, table.putRequest(record))
	}
	// Write batches, respecting the specified maximum batch size
	err := table.writeBatches(ctx, writeRequests)
	if err != nil {
		oldRefId := table.EntityReferenceID(oldVersion)
		newRefId := table.EntityReferenceID(newVersion)
		return fmt.Errorf("failed update from %s to %s: %w", oldRefId, newRefId, err)
	}
	return nil
}

// DeleteEntity deletes the provided entity from the table (including all versions).
func (table Table[T]) DeleteEntity(ctx context.Context, entity T) error {
	entityID := table.EntityID(entity)
	// Delete index row items based on the current version of the entity
	var deleteRecords []Record
	for _, row := range table.IndexRows {
		deleteRecords = append(deleteRecords, row.getDeleteRecords(entity)...)
	}
	// Delete the entity ID from the list of entity IDs
	deleteRecords = append(deleteRecords, Record{
		PartKeyValue: table.EntityRow.getRowPartKey(),
		SortKeyValue: entityID,
	})
	// Delete all versions of the entity from the EntityRow
	partKey := table.EntityRow.getRowPartKeyValue(entityID)
	sortKeys, err := table.ReadAllSortKeyValues(ctx, table.EntityRow, entityID)
	if err != nil {
		return fmt.Errorf("delete entity: failed reading version IDs for %s-%s: %w", table.EntityType, entityID, err)
	}
	for _, sortKey := range sortKeys {
		deleteRecords = append(deleteRecords, Record{
			PartKeyValue: partKey,
			SortKeyValue: sortKey,
		})
	}
	// Write the batch
	writeRequests := Map(deleteRecords, func(r Record) types.WriteRequest { return table.deleteRequest(r) })
	err = table.writeBatches(ctx, writeRequests)
	if err != nil {
		return fmt.Errorf("failed deleting %s-%s: %w", table.EntityType, entityID, err)
	}
	return nil
}

// DeleteEntityWithID deletes the specified entity from the table (including all versions).
func (table Table[T]) DeleteEntityWithID(ctx context.Context, entityID string) (T, error) {
	entity, err := table.ReadEntity(ctx, entityID)
	if err == ErrNotFound {
		return entity, err
	} else if err != nil {
		return entity, fmt.Errorf("failed read to delete %s-%s: %w", table.EntityType, entityID, err)
	}
	return entity, table.DeleteEntity(ctx, entity)
}

// DeleteEntityVersionWithID deletes the specified version of the entity from the table.
func (table Table[T]) DeleteEntityVersionWithID(ctx context.Context, entityID, versionID string) (T, error) {
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

// DeleteRow deletes the entire row for the specified partition key.
func (table Table[T]) DeleteRow(ctx context.Context, row TableRow[T], partKeyValue string) error {
	if partKeyValue == "" {
		return ErrNotFound
	}
	partKey := row.getRowPartKeyValue(partKeyValue)
	sortKeys, err := table.ReadSortKeyValues(ctx, row, partKeyValue, false, 1000, "0")
	if err != nil {
		return fmt.Errorf("failed reading sort keys to delete row %s from table %s: %w", partKey, table.TableName, err)
	}
	for len(sortKeys) > 0 {
		var requests []types.WriteRequest
		for _, sortKey := range sortKeys {
			item := map[string]types.AttributeValue{
				table.getPartKeyAttr(): &types.AttributeValueMemberS{Value: partKey},
				table.getSortKeyAttr(): &types.AttributeValueMemberS{Value: sortKey},
			}
			request := types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: item}}
			requests = append(requests, request)
		}
		err := table.writeBatches(ctx, requests)
		if err != nil {
			return fmt.Errorf("failed deleting row %s from table %s: %w", partKey, table.EntityType, err)
		}
		offset := sortKeys[len(sortKeys)-1]
		sortKeys, err = table.ReadSortKeyValues(ctx, row, partKeyValue, false, 1000, offset)
		if err != nil {
			return fmt.Errorf("failed reading sort keys to delete row %s from table %s: %w", partKey, table.TableName, err)
		}
	}
	return nil
}

// DeleteItem deletes the item with the specified row partition key and sort key.
// Note that this method will seldom be used, perhaps for cleaning up obsolete items.
func (table Table[T]) DeleteItem(ctx context.Context, rowPartKeyValue string, sortKeyValue string) error {
	if rowPartKeyValue == "" || sortKeyValue == "" {
		return ErrNotFound
	}
	item := map[string]types.AttributeValue{
		table.getPartKeyAttr(): &types.AttributeValueMemberS{Value: rowPartKeyValue},
		table.getSortKeyAttr(): &types.AttributeValueMemberS{Value: sortKeyValue},
	}
	req := dynamodb.DeleteItemInput{
		TableName: aws.String(table.TableName),
		Key:       item,
	}
	_, err := table.Client.DeleteItem(ctx, &req)
	if err != nil {
		return fmt.Errorf("failed deleting item %s %s from table %s: %w", rowPartKeyValue, sortKeyValue, table.TableName, err)
	}
	return nil
}

// CountPartKeyValues returns the total number of partition key values in the specified row.
// Note that this may be a slow operation; it pages through all the values to count them.
func (table Table[T]) CountPartKeyValues(ctx context.Context, row TableRow[T]) (int64, error) {
	return table.CountSortKeyValues(ctx, row, "")
}

// ReadAllPartKeyValues reads all the values of the partition keys used for the specified row.
// Note that for some rows, this may be a very large number of values.
func (table Table[T]) ReadAllPartKeyValues(ctx context.Context, row TableRow[T]) ([]string, error) {
	return table.ReadAllSortKeyValues(ctx, row, "")
}

// ReadPartKeyValues reads paginated partition key values for the specified row.
func (table Table[T]) ReadPartKeyValues(ctx context.Context, row TableRow[T], reverse bool, limit int, offset string) ([]string, error) {
	return table.ReadSortKeyValues(ctx, row, "", reverse, limit, offset)
}

// CountSortKeyValues returns the total number of sort key values for the specified row and partition key value.
// Note that this may be a slow operation; it pages through all the values to count them.
func (table Table[T]) CountSortKeyValues(ctx context.Context, row TableRow[T], partKeyValue string) (int64, error) {
	var partKey string
	if partKeyValue == "" {
		// Read partition key values
		partKey = row.getRowPartKey()
	} else {
		// Read sort key values
		partKey = row.getRowPartKeyValue(partKeyValue)
	}
	var count int64
	req := dynamodb.QueryInput{
		TableName: aws.String(table.TableName),
		ExpressionAttributeNames: map[string]string{
			"#p": table.getPartKeyAttr(),
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: partKey},
		},
		KeyConditionExpression: aws.String("#p = :p"),
		ProjectionExpression:   aws.String(table.getSortKeyAttr()),
		ScanIndexForward:       aws.Bool(true),
	}
	paginator := dynamodb.NewQueryPaginator(table.Client, &req)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return count, fmt.Errorf("failed counting sort key values for row %s from table %s: %w", partKey, table.TableName, err)
		}
		count += int64(page.Count)
	}
	return count, nil
}

// ReadAllSortKeyValues reads all the sort key values for the specified row and partition key value.
// Note that for some rows, this may be a very large number of values.
func (table Table[T]) ReadAllSortKeyValues(ctx context.Context, row TableRow[T], partKeyValue string) ([]string, error) {
	var partKey string
	if partKeyValue == "" {
		// Read partition key values
		partKey = row.getRowPartKey()
	} else {
		// Read sort key values
		partKey = row.getRowPartKeyValue(partKeyValue)
	}
	keyValues := make([]string, 0)
	req := dynamodb.QueryInput{
		TableName: aws.String(table.TableName),
		ExpressionAttributeNames: map[string]string{
			"#p": table.getPartKeyAttr(),
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: partKey},
		},
		KeyConditionExpression: aws.String("#p = :p"),
		ProjectionExpression:   aws.String(table.getSortKeyAttr()),
		ScanIndexForward:       aws.Bool(true),
	}
	paginator := dynamodb.NewQueryPaginator(table.Client, &req)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return keyValues, fmt.Errorf("failed reading all sort key values for row %s from table %s: %w", partKey, table.TableName, err)
		}
		for _, item := range page.Items {
			if item == nil || item[table.getSortKeyAttr()] == nil {
				continue
			}
			attrValue := item[table.getSortKeyAttr()]
			keyValue := attrValue.(*types.AttributeValueMemberS).Value
			if keyValue != "" {
				keyValues = append(keyValues, keyValue)
			}
		}
	}
	return keyValues, nil
}

// ReadSortKeyValues reads paginated sort key values for the specified row and partition key value.
func (table Table[T]) ReadSortKeyValues(ctx context.Context, row TableRow[T], partKeyValue string, reverse bool, limit int, offset string) ([]string, error) {
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
	keyValues := make([]string, 0)
	keyConditionExpression := "#p = :p and #s > :s"
	if reverse {
		keyConditionExpression = "#p = :p and #s < :s"
	}
	req := dynamodb.QueryInput{
		TableName: aws.String(table.TableName),
		ExpressionAttributeNames: map[string]string{
			"#p": table.getPartKeyAttr(),
			"#s": table.getSortKeyAttr(),
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: partKey},
			":s": &types.AttributeValueMemberS{Value: offset},
		},
		KeyConditionExpression: aws.String(keyConditionExpression),
		ProjectionExpression:   aws.String(table.getSortKeyAttr()),
		ScanIndexForward:       aws.Bool(!reverse),
		Limit:                  aws.Int32(int32(limit)),
	}
	res, err := table.Client.Query(ctx, &req)
	if err != nil {
		return keyValues, fmt.Errorf("failed reading sort key values for row %s from table %s: %w", partKey, table.TableName, err)
	}
	for _, item := range res.Items {
		if item == nil || item[table.getSortKeyAttr()] == nil {
			continue
		}
		attrValue := item[table.getSortKeyAttr()]
		keyValue := attrValue.(*types.AttributeValueMemberS).Value
		if keyValue != "" {
			keyValues = append(keyValues, keyValue)
		}
	}
	return keyValues, nil
}

// ReadFirstSortKeyValue reads the first sort key value for the specified row and partition key value.
// This method is typically used for looking up an ID corresponding to a foreign key.
func (table Table[T]) ReadFirstSortKeyValue(ctx context.Context, row TableRow[T], partKeyValue string) (string, error) {
	var partKey string
	if partKeyValue == "" {
		// Read partition key values
		partKey = row.getRowPartKey()
	} else {
		// Read sort key values
		partKey = row.getRowPartKeyValue(partKeyValue)
	}
	req := dynamodb.QueryInput{
		TableName: aws.String(table.TableName),
		ExpressionAttributeNames: map[string]string{
			"#p": table.getPartKeyAttr(),
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: partKey},
		},
		KeyConditionExpression: aws.String("#p = :p"),
		ProjectionExpression:   aws.String(table.getSortKeyAttr()),
		ScanIndexForward:       aws.Bool(true),
		Limit:                  aws.Int32(1),
	}
	res, err := table.Client.Query(ctx, &req)
	if err != nil {
		return "", fmt.Errorf("error reading first sort key value for %s: %w", partKey, err)
	}
	if len(res.Items) == 0 || res.Items[0][table.getSortKeyAttr()] == nil {
		return "", ErrNotFound
	}
	return res.Items[0][table.getSortKeyAttr()].(*types.AttributeValueMemberS).Value, nil
}

// ReadLastSortKeyValue reads the last sort key value for the specified row and partition key value.
// This method is typically used for looking up the current version ID of a versioned entity.
func (table Table[T]) ReadLastSortKeyValue(ctx context.Context, row TableRow[T], partKeyValue string) (string, error) {
	var partKey string
	if partKeyValue == "" {
		// Read partition key values
		partKey = row.getRowPartKey()
	} else {
		// Read sort key values
		partKey = row.getRowPartKeyValue(partKeyValue)
	}
	req := dynamodb.QueryInput{
		TableName: aws.String(table.TableName),
		ExpressionAttributeNames: map[string]string{
			"#p": table.getPartKeyAttr(),
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: partKey},
		},
		KeyConditionExpression: aws.String("#p = :p"),
		ProjectionExpression:   aws.String(table.getSortKeyAttr()),
		ScanIndexForward:       aws.Bool(false),
		Limit:                  aws.Int32(1),
	}
	res, err := table.Client.Query(ctx, &req)
	if err != nil {
		return "", fmt.Errorf("error reading last sort key value for %s: %w", partKey, err)
	}
	if len(res.Items) == 0 || res.Items[0][table.getSortKeyAttr()] == nil {
		return "", ErrNotFound
	}
	return res.Items[0][table.getSortKeyAttr()].(*types.AttributeValueMemberS).Value, nil
}

// ReadAllEntityIDs reads all entity IDs from the EntityRow.
// Note that this can return a very large number of IDs!
func (table Table[T]) ReadAllEntityIDs(ctx context.Context) ([]string, error) {
	return table.ReadAllPartKeyValues(ctx, table.EntityRow)
}

// ReadEntityIDs reads paginated entity IDs from the EntityRow.
func (table Table[T]) ReadEntityIDs(ctx context.Context, reverse bool, limit int, offset string) ([]string, error) {
	return table.ReadPartKeyValues(ctx, table.EntityRow, reverse, limit, offset)
}

// ReadAllEntityVersionIDs reads all entity version IDs for the specified entity.
// Note that this can return a very large number of IDs!
func (table Table[T]) ReadAllEntityVersionIDs(ctx context.Context, entityID string) ([]string, error) {
	return table.ReadAllSortKeyValues(ctx, table.EntityRow, entityID)
}

// ReadEntityVersionIDs reads paginated entity version IDs for the specified entity.
func (table Table[T]) ReadEntityVersionIDs(ctx context.Context, entityID string, reverse bool, limit int, offset string) ([]string, error) {
	return table.ReadSortKeyValues(ctx, table.EntityRow, entityID, reverse, limit, offset)
}

// ReadCurrentEntityVersionID reads the current entity version ID for the specified entity.
func (table Table[T]) ReadCurrentEntityVersionID(ctx context.Context, entityID string) (string, error) {
	return table.ReadLastSortKeyValue(ctx, table.EntityRow, entityID)
}

// ReadEntities reads the current versions of the specified entities from the EntityRow.
// If an entity does not exist, it will be omitted from the results.
func (table Table[T]) ReadEntities(ctx context.Context, entityIDs []string) []T {
	entities := make([]T, 0, len(entityIDs))
	if len(entityIDs) == 0 {
		return entities
	}
	entityIndex := make(map[string]T, len(entityIDs))
	var batches [][]string
	maxBatchSize := 10 // concurrent request limit
	batches = Batch(entityIDs, maxBatchSize)
	for _, batch := range batches {
		batchSize := len(batch)
		results := make(chan struct {
			ID     string
			entity T
		}, batchSize)
		var wg sync.WaitGroup
		wg.Add(batchSize)
		for _, entityID := range batch {
			go func(entityID string) {
				defer wg.Done()
				entity, err := table.ReadEntity(ctx, entityID)
				if err != nil && err != ErrNotFound {
					log.Printf("failed concurrent read %s-%s: %v\n", table.EntityType, entityID, err)
				} else if err != ErrNotFound {
					results <- struct {
						ID     string
						entity T
					}{ID: entityID, entity: entity}
				}
			}(entityID)
		}
		wg.Wait()
		close(results)
		for entity := range results {
			entityIndex[entity.ID] = entity.entity
		}
	}
	for _, id := range entityIDs {
		if val, ok := entityIndex[id]; ok {
			entities = append(entities, val)
		}
	}
	return entities
}

// ReadEntitiesAsJSON reads the current versions of the specified entities from the EntityRow as a JSON byte slice.
// If an entity does not exist, it will be omitted from the results.
func (table Table[T]) ReadEntitiesAsJSON(ctx context.Context, entityIDs []string) []byte {
	if len(entityIDs) == 0 {
		return nil
	}
	entityIndex := make(map[string][]byte, len(entityIDs))
	var batches [][]string
	maxBatchSize := 10 // concurrent request limit
	batches = Batch(entityIDs, maxBatchSize)
	for _, batch := range batches {
		batchSize := len(batch)
		results := make(chan struct {
			ID     string
			entity []byte
		}, batchSize)
		var wg sync.WaitGroup
		wg.Add(batchSize)
		for _, entityID := range batch {
			go func(entityID string) {
				defer wg.Done()
				entity, err := table.ReadEntityAsJSON(ctx, entityID)
				if err != nil && err != ErrNotFound {
					log.Printf("failed concurrent read as JSON %s-%s: %v\n", table.EntityType, entityID, err)
				} else if err != ErrNotFound {
					results <- struct {
						ID     string
						entity []byte
					}{ID: entityID, entity: entity}
				}
			}(entityID)
		}
		wg.Wait()
		close(results)
		for entity := range results {
			entityIndex[entity.ID] = entity.entity
		}
	}
	var entities bytes.Buffer
	entities.WriteString("[")
	var i int
	for _, id := range entityIDs {
		if val, ok := entityIndex[id]; ok {
			if i > 0 {
				entities.WriteString(",")
			}
			entities.Write(val)
			i++
		}
	}
	entities.WriteString("]")
	return entities.Bytes()
}

// ReadEntity reads the current version of the specified entity.
func (table Table[T]) ReadEntity(ctx context.Context, entityID string) (T, error) {
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
func (table Table[T]) ReadEntityAsJSON(ctx context.Context, entityID string) ([]byte, error) {
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
func (table Table[T]) ReadEntityAsCompressedJSON(ctx context.Context, entityID string) ([]byte, error) {
	if table.EntityRow.JsonValue == nil {
		return nil, errors.New("the EntityRow must contain compressed JSON values")
	}
	if entityID == "" {
		return nil, ErrNotFound
	}
	// Read the current version of a versioned entity
	req := dynamodb.QueryInput{
		TableName: aws.String(table.TableName),
		ExpressionAttributeNames: map[string]string{
			"#p": table.getPartKeyAttr(),
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: table.EntityRow.getRowPartKeyValue(entityID)},
		},
		KeyConditionExpression: aws.String("#p = :p"),
		ProjectionExpression:   aws.String(table.getJsonValueAttr()),
		ScanIndexForward:       aws.Bool(false),
		Limit:                  aws.Int32(1),
	}
	res, err := table.Client.Query(ctx, &req)
	if err != nil {
		return nil, fmt.Errorf("error reading %s-%s: %w", table.EntityType, entityID, err)
	}
	if len(res.Items) == 0 || res.Items[0][table.getJsonValueAttr()] == nil {
		return nil, ErrNotFound
	}
	return res.Items[0][table.getJsonValueAttr()].(*types.AttributeValueMemberB).Value, nil
}

// ReadEntityVersion reads the specified version of the specified entity.
func (table Table[T]) ReadEntityVersion(ctx context.Context, entityID string, versionID string) (T, error) {
	return table.ReadEntityFromRow(ctx, table.EntityRow, entityID, versionID)
}

// ReadEntityVersionAsJSON reads the specified version of the specified entity as a JSON byte slice.
func (table Table[T]) ReadEntityVersionAsJSON(ctx context.Context, entityID string, versionID string) ([]byte, error) {
	return table.ReadEntityFromRowAsJSON(ctx, table.EntityRow, entityID, versionID)
}

// ReadEntityVersions reads paginated versions of the specified entity.
func (table Table[T]) ReadEntityVersions(ctx context.Context, entityID string, reverse bool, limit int, offset string) ([]T, error) {
	return table.ReadEntitiesFromRow(ctx, table.EntityRow, entityID, reverse, limit, offset)
}

// ReadEntityVersionsAsJSON reads paginated versions of the specified entity as a JSON byte slice.
func (table Table[T]) ReadEntityVersionsAsJSON(ctx context.Context, entityID string, reverse bool, limit int, offset string) ([]byte, error) {
	return table.ReadEntitiesFromRowAsJSON(ctx, table.EntityRow, entityID, reverse, limit, offset)
}

// ReadAllEntityVersions reads all versions of the specified entity.
// Note: this can return a very large number of versions! Use with caution.
func (table Table[T]) ReadAllEntityVersions(ctx context.Context, entityID string) ([]T, error) {
	return table.ReadAllEntitiesFromRow(ctx, table.EntityRow, entityID)
}

// ReadAllEntityVersionsAsJSON reads all versions of the specified entity as a JSON byte slice.
// Note: this can return a very large number of versions! Use with caution.
func (table Table[T]) ReadAllEntityVersionsAsJSON(ctx context.Context, entityID string) ([]byte, error) {
	return table.ReadAllEntitiesFromRowAsJSON(ctx, table.EntityRow, entityID)
}

// ReadEntityFromRow reads the specified entity from the specified row.
func (table Table[T]) ReadEntityFromRow(ctx context.Context, row TableRow[T], partKeyValue string, sortKeyValue string) (T, error) {
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
func (table Table[T]) ReadEntityFromRowAsJSON(ctx context.Context, row TableRow[T], partKeyValue string, sortKeyValue string) ([]byte, error) {
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
func (table Table[T]) ReadEntityFromRowAsCompressedJSON(ctx context.Context, row TableRow[T], partKeyValue string, sortKeyValue string) ([]byte, error) {
	if row.JsonValue == nil {
		return nil, errors.New("row " + row.RowName + " must contain compressed JSON values")
	}
	if partKeyValue == "" || sortKeyValue == "" {
		return nil, ErrNotFound
	}
	req := dynamodb.GetItemInput{
		TableName: aws.String(table.TableName),
		Key: map[string]types.AttributeValue{
			table.getPartKeyAttr(): &types.AttributeValueMemberS{Value: row.getRowPartKeyValue(partKeyValue)},
			table.getSortKeyAttr(): &types.AttributeValueMemberS{Value: sortKeyValue},
		},
		ProjectionExpression: aws.String(table.getJsonValueAttr()),
	}
	res, err := table.Client.GetItem(ctx, &req)
	if err != nil {
		return nil, fmt.Errorf("error reading %s %s %s: %w",
			table.EntityType, row.getRowPartKeyValue(partKeyValue), sortKeyValue, err)
	}
	if res.Item == nil || res.Item[table.getJsonValueAttr()] == nil {
		return nil, ErrNotFound
	}
	return res.Item[table.getJsonValueAttr()].(*types.AttributeValueMemberB).Value, nil
}

// ReadEntitiesFromRow reads paginated entities from the specified row.
func (table Table[T]) ReadEntitiesFromRow(ctx context.Context, row TableRow[T], partKeyValue string, reverse bool, limit int, offset string) ([]T, error) {
	entities := make([]T, 0)
	if row.JsonValue == nil {
		return entities, errors.New("row " + row.RowName + " must contain compressed JSON values")
	}
	if partKeyValue == "" {
		return entities, nil
	}
	if offset == "" {
		if reverse {
			offset = "|" // after letters
		} else {
			offset = "-" // before numbers
		}
	}
	keyConditionExpression := "#p = :p and #s > :s"
	if reverse {
		keyConditionExpression = "#p = :p and #s < :s"
	}
	req := dynamodb.QueryInput{
		TableName: aws.String(table.TableName),
		ExpressionAttributeNames: map[string]string{
			"#p": table.getPartKeyAttr(),
			"#s": table.getSortKeyAttr(),
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: row.getRowPartKeyValue(partKeyValue)},
			":s": &types.AttributeValueMemberS{Value: offset},
		},
		KeyConditionExpression: aws.String(keyConditionExpression),
		ProjectionExpression:   aws.String(table.getJsonValueAttr()),
		ScanIndexForward:       aws.Bool(!reverse),
		Limit:                  aws.Int32(int32(limit)),
	}
	res, err := table.Client.Query(ctx, &req)
	if err != nil {
		return entities, fmt.Errorf("error reading paginated %s %s: %w",
			table.EntityType, row.getRowPartKeyValue(partKeyValue), err)
	}
	for _, item := range res.Items {
		if item == nil || item[table.getJsonValueAttr()] == nil {
			continue
		}
		gzJSON := item[table.getJsonValueAttr()].(*types.AttributeValueMemberB).Value
		if entity, err := FromCompressedJSON[T](gzJSON); err == nil {
			// best effort; skip entities with broken JSON
			entities = append(entities, entity)
		}
	}
	return entities, nil
}

// ReadEntitiesFromRowAsJSON reads paginated entities from the specified row, returning a JSON byte slice.
func (table Table[T]) ReadEntitiesFromRowAsJSON(ctx context.Context, row TableRow[T], partKeyValue string, reverse bool, limit int, offset string) ([]byte, error) {
	if row.JsonValue == nil {
		return nil, errors.New("row " + row.RowName + " must contain compressed JSON values")
	}
	if partKeyValue == "" {
		return []byte("[]"), nil
	}
	if offset == "" {
		if reverse {
			offset = "|" // after letters
		} else {
			offset = "-" // before numbers
		}
	}
	keyConditionExpression := "#p = :p and #s > :s"
	if reverse {
		keyConditionExpression = "#p = :p and #s < :s"
	}
	req := dynamodb.QueryInput{
		TableName: aws.String(table.TableName),
		ExpressionAttributeNames: map[string]string{
			"#p": table.getPartKeyAttr(),
			"#s": table.getSortKeyAttr(),
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: row.getRowPartKeyValue(partKeyValue)},
			":s": &types.AttributeValueMemberS{Value: offset},
		},
		KeyConditionExpression: aws.String(keyConditionExpression),
		ProjectionExpression:   aws.String(table.getJsonValueAttr()),
		ScanIndexForward:       aws.Bool(!reverse),
		Limit:                  aws.Int32(int32(limit)),
	}
	res, err := table.Client.Query(ctx, &req)
	if err != nil {
		return []byte("[]"), fmt.Errorf("error reading paginated JSON %s %s: %w",
			table.EntityType, row.getRowPartKeyValue(partKeyValue), err)
	}
	var i int
	var buf bytes.Buffer
	buf.WriteString("[")
	for _, item := range res.Items {
		if item == nil || item[table.getJsonValueAttr()] == nil {
			continue
		}
		gzJSON := item[table.getJsonValueAttr()].(*types.AttributeValueMemberB).Value
		if jsonBytes, err := UncompressJSON(gzJSON); err == nil {
			// best effort; skip entities with broken compressed JSON
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
func (table Table[T]) ReadAllEntitiesFromRow(ctx context.Context, row TableRow[T], partKeyValue string) ([]T, error) {
	entities := make([]T, 0)
	if row.JsonValue == nil {
		return entities, errors.New("row " + row.RowName + " must contain compressed JSON values")
	}
	if partKeyValue == "" {
		return entities, nil
	}
	req := dynamodb.QueryInput{
		TableName: aws.String(table.TableName),
		ExpressionAttributeNames: map[string]string{
			"#p": table.getPartKeyAttr(),
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: row.getRowPartKeyValue(partKeyValue)},
		},
		KeyConditionExpression: aws.String("#p = :p"),
		ProjectionExpression:   aws.String(table.getJsonValueAttr()),
		ScanIndexForward:       aws.Bool(true),
	}
	paginator := dynamodb.NewQueryPaginator(table.Client, &req)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return entities, fmt.Errorf("error reading all %s %s: %w",
				table.EntityType, row.getRowPartKeyValue(partKeyValue), err)
		}
		for _, item := range page.Items {
			if item == nil || item[table.getJsonValueAttr()] == nil {
				continue
			}
			gzJSON := item[table.getJsonValueAttr()].(*types.AttributeValueMemberB).Value
			if entity, err := FromCompressedJSON[T](gzJSON); err == nil {
				// best effort; skip entities with broken JSON
				entities = append(entities, entity)
			}
		}
	}
	return entities, nil
}

// ReadAllEntitiesFromRowAsJSON reads all entities from the specified row as a JSON byte slice.
// Note: this could be a lot of data! It should only be used for small collections.
func (table Table[T]) ReadAllEntitiesFromRowAsJSON(ctx context.Context, row TableRow[T], partKeyValue string) ([]byte, error) {
	if row.JsonValue == nil {
		return nil, errors.New("row " + row.RowName + " must contain compressed JSON values")
	}
	if partKeyValue == "" {
		return []byte("[]"), nil
	}
	req := dynamodb.QueryInput{
		TableName: aws.String(table.TableName),
		ExpressionAttributeNames: map[string]string{
			"#p": table.getPartKeyAttr(),
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: row.getRowPartKeyValue(partKeyValue)},
		},
		KeyConditionExpression: aws.String("#p = :p"),
		ProjectionExpression:   aws.String(table.getJsonValueAttr()),
		ScanIndexForward:       aws.Bool(true),
	}
	paginator := dynamodb.NewQueryPaginator(table.Client, &req)
	var comma bool
	var buf bytes.Buffer
	buf.WriteString("[")
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return []byte("[]"), fmt.Errorf("error reading all JSON %s %s: %w",
				table.EntityType, row.getRowPartKeyValue(partKeyValue), err)
		}
		for _, item := range page.Items {
			if item == nil || item[table.getJsonValueAttr()] == nil {
				continue
			}
			gzJSON := item[table.getJsonValueAttr()].(*types.AttributeValueMemberB).Value
			if jsonBytes, err := UncompressJSON(gzJSON); err == nil {
				// best effort; skip entities with broken compressed JSON
				if comma {
					buf.WriteString(",")
				}
				buf.Write(jsonBytes)
				comma = true
			}
		}
	}
	buf.WriteString("]")
	return buf.Bytes(), nil
}

// ReadRecord reads the specified record from the provided table row.
func (table Table[T]) ReadRecord(ctx context.Context, row TableRow[T], partKeyValue string, sortKeyValue string) (Record, error) {
	if partKeyValue == "" || sortKeyValue == "" {
		return Record{}, ErrNotFound
	}
	attributes := strings.Join(table.getAttributeNames(), ", ")
	req := dynamodb.GetItemInput{
		TableName: aws.String(table.TableName),
		Key: map[string]types.AttributeValue{
			table.getPartKeyAttr(): &types.AttributeValueMemberS{Value: row.getRowPartKeyValue(partKeyValue)},
			table.getSortKeyAttr(): &types.AttributeValueMemberS{Value: sortKeyValue},
		},
		ProjectionExpression: aws.String(attributes),
	}
	res, err := table.Client.GetItem(ctx, &req)
	if err != nil {
		return Record{}, fmt.Errorf("error reading record %s %s %s: %w",
			table.EntityType, row.getRowPartKeyValue(partKeyValue), sortKeyValue, err)
	}
	if len(res.Item) == 0 {
		return Record{}, ErrNotFound
	}
	return table.itemToRecord(res.Item), nil
}

// ReadRecords reads paginated Records from the specified row.
func (table Table[T]) ReadRecords(ctx context.Context, row TableRow[T], partKeyValue string, reverse bool, limit int, offset string) ([]Record, error) {
	records := make([]Record, 0)
	if partKeyValue == "" {
		return records, nil
	}
	if offset == "" {
		if reverse {
			offset = "|" // after letters
		} else {
			offset = "-" // before numbers
		}
	}
	keyConditionExpression := "#p = :p and #s > :s"
	if reverse {
		keyConditionExpression = "#p = :p and #s < :s"
	}
	attributes := strings.Join(table.getAttributeNames(), ", ")
	req := dynamodb.QueryInput{
		TableName: aws.String(table.TableName),
		ExpressionAttributeNames: map[string]string{
			"#p": table.getPartKeyAttr(),
			"#s": table.getSortKeyAttr(),
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: row.getRowPartKeyValue(partKeyValue)},
			":s": &types.AttributeValueMemberS{Value: offset},
		},
		KeyConditionExpression: aws.String(keyConditionExpression),
		ProjectionExpression:   aws.String(attributes),
		ScanIndexForward:       aws.Bool(!reverse),
		Limit:                  aws.Int32(int32(limit)),
	}
	res, err := table.Client.Query(ctx, &req)
	if err != nil {
		return records, fmt.Errorf("error reading paginated records %s %s: %w",
			table.EntityType, row.getRowPartKeyValue(partKeyValue), err)
	}
	for _, item := range res.Items {
		if len(item) > 0 {
			records = append(records, table.itemToRecord(item))
		}
	}
	return records, nil
}

// ReadAllRecords reads all Records from the specified row.
// Note: this can return a very large number of values! Use with caution.
func (table Table[T]) ReadAllRecords(ctx context.Context, row TableRow[T], partKeyValue string) ([]Record, error) {
	records := make([]Record, 0)
	if partKeyValue == "" {
		return records, nil
	}
	attributes := strings.Join(table.getAttributeNames(), ", ")
	req := dynamodb.QueryInput{
		TableName: aws.String(table.TableName),
		ExpressionAttributeNames: map[string]string{
			"#p": table.getPartKeyAttr(),
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: row.getRowPartKeyValue(partKeyValue)},
		},
		KeyConditionExpression: aws.String("#p = :p"),
		ProjectionExpression:   aws.String(attributes),
		ScanIndexForward:       aws.Bool(true),
	}
	paginator := dynamodb.NewQueryPaginator(table.Client, &req)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return records, fmt.Errorf("error reading all records %s %s: %w",
				table.EntityType, row.getRowPartKeyValue(partKeyValue), err)
		}
		for _, item := range page.Items {
			if len(item) > 0 {
				records = append(records, table.itemToRecord(item))
			}
		}
	}
	return records, nil
}

// ReadTextValue reads the specified text value from the specified row.
func (table Table[T]) ReadTextValue(ctx context.Context, row TableRow[T], partKeyValue string, sortKeyValue string) (TextValue, error) {
	textValue := TextValue{Key: sortKeyValue, Value: ""}
	if row.TextValue == nil {
		return textValue, errors.New("row " + row.RowName + " must contain text values")
	}
	if partKeyValue == "" || sortKeyValue == "" {
		return textValue, ErrNotFound
	}
	req := dynamodb.GetItemInput{
		TableName: aws.String(table.TableName),
		Key: map[string]types.AttributeValue{
			table.getPartKeyAttr(): &types.AttributeValueMemberS{Value: row.getRowPartKeyValue(partKeyValue)},
			table.getSortKeyAttr(): &types.AttributeValueMemberS{Value: sortKeyValue},
		},
		ProjectionExpression: aws.String(table.getSortKeyAttr() + ", " + table.getTextValueAttr()),
	}
	res, err := table.Client.GetItem(ctx, &req)
	if err != nil {
		return textValue, fmt.Errorf("error reading text value %s %s %s: %w",
			table.EntityType, row.getRowPartKeyValue(partKeyValue), sortKeyValue, err)
	}
	if res.Item == nil || res.Item[table.getSortKeyAttr()] == nil || res.Item[table.getTextValueAttr()] == nil {
		return textValue, ErrNotFound
	}
	textValue.Key = res.Item[table.getSortKeyAttr()].(*types.AttributeValueMemberS).Value
	textValue.Value = res.Item[table.getTextValueAttr()].(*types.AttributeValueMemberS).Value
	return textValue, nil
}

// ReadTextValues reads paginated text values from the specified row.
func (table Table[T]) ReadTextValues(ctx context.Context, row TableRow[T], partKeyValue string, reverse bool, limit int, offset string) ([]TextValue, error) {
	textValues := make([]TextValue, 0)
	if row.TextValue == nil {
		return textValues, errors.New("row " + row.RowName + " must contain text values")
	}
	if partKeyValue == "" {
		return textValues, nil
	}
	if offset == "" {
		if reverse {
			offset = "|" // after letters
		} else {
			offset = "-" // before numbers
		}
	}
	keyConditionExpression := "#p = :p and #s > :s"
	if reverse {
		keyConditionExpression = "#p = :p and #s < :s"
	}
	req := dynamodb.QueryInput{
		TableName: aws.String(table.TableName),
		ExpressionAttributeNames: map[string]string{
			"#p": table.getPartKeyAttr(),
			"#s": table.getSortKeyAttr(),
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: row.getRowPartKeyValue(partKeyValue)},
			":s": &types.AttributeValueMemberS{Value: offset},
		},
		KeyConditionExpression: aws.String(keyConditionExpression),
		ProjectionExpression:   aws.String(table.getSortKeyAttr() + ", " + table.getTextValueAttr()),
		ScanIndexForward:       aws.Bool(!reverse),
		Limit:                  aws.Int32(int32(limit)),
	}
	res, err := table.Client.Query(ctx, &req)
	if err != nil {
		return textValues, fmt.Errorf("error reading paginated text values %s %s: %w",
			table.EntityType, row.getRowPartKeyValue(partKeyValue), err)
	}
	for _, item := range res.Items {
		if item != nil && item[table.getSortKeyAttr()] != nil && item[table.getTextValueAttr()] != nil {
			textValues = append(textValues, TextValue{
				Key:   item[table.getSortKeyAttr()].(*types.AttributeValueMemberS).Value,
				Value: item[table.getTextValueAttr()].(*types.AttributeValueMemberS).Value,
			})
		}
	}
	return textValues, nil
}

// ReadAllTextValues reads all text values from the specified row.
// Note: this can return a very large number of values! Use with caution.
func (table Table[T]) ReadAllTextValues(ctx context.Context, row TableRow[T], partKeyValue string, sortByValue bool) ([]TextValue, error) {
	textValues := make([]TextValue, 0)
	if row.TextValue == nil {
		return textValues, errors.New("row " + row.RowName + " must contain text values")
	}
	if partKeyValue == "" {
		return textValues, nil
	}
	req := dynamodb.QueryInput{
		TableName: aws.String(table.TableName),
		ExpressionAttributeNames: map[string]string{
			"#p": table.getPartKeyAttr(),
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: row.getRowPartKeyValue(partKeyValue)},
		},
		KeyConditionExpression: aws.String("#p = :p"),
		ProjectionExpression:   aws.String(table.getSortKeyAttr() + ", " + table.getTextValueAttr()),
		ScanIndexForward:       aws.Bool(true),
	}
	paginator := dynamodb.NewQueryPaginator(table.Client, &req)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return textValues, fmt.Errorf("error reading all text values %s %s: %w",
				table.EntityType, row.getRowPartKeyValue(partKeyValue), err)
		}
		for _, item := range page.Items {
			if item != nil && item[table.getSortKeyAttr()] != nil && item[table.getTextValueAttr()] != nil {
				textValues = append(textValues, TextValue{
					Key:   item[table.getSortKeyAttr()].(*types.AttributeValueMemberS).Value,
					Value: item[table.getTextValueAttr()].(*types.AttributeValueMemberS).Value,
				})
			}
		}
	}
	if sortByValue {
		sort.Slice(textValues, func(i, j int) bool {
			return textValues[i].Value < textValues[j].Value
		})
	}
	return textValues, nil
}

// FilterTextValues returns all text values from the specified row that match the specified filter.
// The resulting text values are sorted by value.
func (table Table[T]) FilterTextValues(ctx context.Context, row TableRow[T], partKeyValue string, f func(TextValue) bool) ([]TextValue, error) {
	textValues := make([]TextValue, 0)
	if row.TextValue == nil {
		return textValues, errors.New("row " + row.RowName + " must contain text values")
	}
	if partKeyValue == "" {
		return textValues, nil
	}
	req := dynamodb.QueryInput{
		TableName: aws.String(table.TableName),
		ExpressionAttributeNames: map[string]string{
			"#p": table.getPartKeyAttr(),
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: row.getRowPartKeyValue(partKeyValue)},
		},
		KeyConditionExpression: aws.String("#p = :p"),
		ProjectionExpression:   aws.String(table.getSortKeyAttr() + ", " + table.getTextValueAttr()),
		ScanIndexForward:       aws.Bool(true),
	}
	paginator := dynamodb.NewQueryPaginator(table.Client, &req)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return textValues, fmt.Errorf("error filtering text values %s %s: %w",
				table.EntityType, row.getRowPartKeyValue(partKeyValue), err)
		}
		for _, item := range page.Items {
			if item != nil && item[table.getSortKeyAttr()] != nil && item[table.getTextValueAttr()] != nil {
				tv := TextValue{
					Key:   item[table.getSortKeyAttr()].(*types.AttributeValueMemberS).Value,
					Value: item[table.getTextValueAttr()].(*types.AttributeValueMemberS).Value,
				}
				if f(tv) {
					textValues = append(textValues, tv)
				}
			}
		}
	}
	sort.Slice(textValues, func(i, j int) bool {
		return textValues[i].Value < textValues[j].Value
	})
	return textValues, nil
}

// ReadNumericValue reads the specified numeric value from the specified row.
func (table Table[T]) ReadNumericValue(ctx context.Context, row TableRow[T], partKeyValue string, sortKeyValue string) (NumValue, error) {
	numValue := NumValue{Key: sortKeyValue, Value: 0}
	if row.NumericValue == nil {
		return numValue, errors.New("row " + row.RowName + " must contain numeric values")
	}
	if partKeyValue == "" || sortKeyValue == "" {
		return numValue, ErrNotFound
	}
	req := dynamodb.GetItemInput{
		TableName: aws.String(table.TableName),
		Key: map[string]types.AttributeValue{
			table.getPartKeyAttr(): &types.AttributeValueMemberS{Value: row.getRowPartKeyValue(partKeyValue)},
			table.getSortKeyAttr(): &types.AttributeValueMemberS{Value: sortKeyValue},
		},
		ProjectionExpression: aws.String(table.getSortKeyAttr() + ", " + table.getNumericValueAttr()),
	}
	res, err := table.Client.GetItem(ctx, &req)
	if err != nil {
		return numValue, fmt.Errorf("error reading numeric value %s %s %s: %w",
			table.EntityType, row.getRowPartKeyValue(partKeyValue), sortKeyValue, err)
	}
	if res.Item == nil || res.Item[table.getSortKeyAttr()] == nil || res.Item[table.getNumericValueAttr()] == nil {
		return numValue, ErrNotFound
	}
	numValue.Key = res.Item[table.getSortKeyAttr()].(*types.AttributeValueMemberS).Value
	numValue.Value, _ = strconv.ParseFloat(res.Item[table.getNumericValueAttr()].(*types.AttributeValueMemberN).Value, 64)
	return numValue, nil
}

// ReadNumericValues reads paginated numeric values from the specified row.
func (table Table[T]) ReadNumericValues(ctx context.Context, row TableRow[T], partKeyValue string, reverse bool, limit int, offset string) ([]NumValue, error) {
	numValues := make([]NumValue, 0)
	if row.NumericValue == nil {
		return numValues, errors.New("row " + row.RowName + " must contain numeric values")
	}
	if partKeyValue == "" {
		return numValues, nil
	}
	if offset == "" {
		if reverse {
			offset = "|" // after letters
		} else {
			offset = "-" // before numbers
		}
	}
	keyConditionExpression := "#p = :p and #s > :s"
	if reverse {
		keyConditionExpression = "#p = :p and #s < :s"
	}
	req := dynamodb.QueryInput{
		TableName: aws.String(table.TableName),
		ExpressionAttributeNames: map[string]string{
			"#p": table.getPartKeyAttr(),
			"#s": table.getSortKeyAttr(),
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: row.getRowPartKeyValue(partKeyValue)},
			":s": &types.AttributeValueMemberS{Value: offset},
		},
		KeyConditionExpression: aws.String(keyConditionExpression),
		ProjectionExpression:   aws.String(table.getSortKeyAttr() + ", " + table.getNumericValueAttr()),
		ScanIndexForward:       aws.Bool(!reverse),
		Limit:                  aws.Int32(int32(limit)),
	}
	res, err := table.Client.Query(ctx, &req)
	if err != nil {
		return numValues, fmt.Errorf("error reading paginated numeric values %s %s: %w",
			table.EntityType, row.getRowPartKeyValue(partKeyValue), err)
	}
	for _, item := range res.Items {
		if item != nil && item[table.getSortKeyAttr()] != nil && item[table.getNumericValueAttr()] != nil {
			key := item[table.getSortKeyAttr()].(*types.AttributeValueMemberS).Value
			value, _ := strconv.ParseFloat(item[table.getNumericValueAttr()].(*types.AttributeValueMemberN).Value, 64)
			numValues = append(numValues, NumValue{Key: key, Value: value})
		}
	}
	return numValues, nil
}

// ReadAllNumericValues reads all numeric values from the specified row.
// Note: this can return a very large number of values! Use with caution.
func (table Table[T]) ReadAllNumericValues(ctx context.Context, row TableRow[T], partKeyValue string, sortByValue bool) ([]NumValue, error) {
	numValues := make([]NumValue, 0)
	if row.NumericValue == nil {
		return numValues, errors.New("row " + row.RowName + " must contain numeric values")
	}
	if partKeyValue == "" {
		return numValues, nil
	}
	req := dynamodb.QueryInput{
		TableName: aws.String(table.TableName),
		ExpressionAttributeNames: map[string]string{
			"#p": table.getPartKeyAttr(),
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: row.getRowPartKeyValue(partKeyValue)},
		},
		KeyConditionExpression: aws.String("#p = :p"),
		ProjectionExpression:   aws.String(table.getSortKeyAttr() + ", " + table.getNumericValueAttr()),
		ScanIndexForward:       aws.Bool(true),
	}
	paginator := dynamodb.NewQueryPaginator(table.Client, &req)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return numValues, fmt.Errorf("error reading all numeric values %s %s: %w",
				table.EntityType, row.getRowPartKeyValue(partKeyValue), err)
		}
		for _, item := range page.Items {
			if item != nil && item[table.getSortKeyAttr()] != nil && item[table.getNumericValueAttr()] != nil {
				key := item[table.getSortKeyAttr()].(*types.AttributeValueMemberS).Value
				value, _ := strconv.ParseFloat(item[table.getNumericValueAttr()].(*types.AttributeValueMemberN).Value, 64)
				numValues = append(numValues, NumValue{Key: key, Value: value})
			}
		}
	}
	if sortByValue {
		sort.Slice(numValues, func(i, j int) bool {
			return numValues[i].Value < numValues[j].Value
		})
	}
	return numValues, nil
}

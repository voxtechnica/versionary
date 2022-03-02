package versionary

import (
	"bytes"
	"context"
	"errors"
	"log"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
	"github.com/voxtechnica/tuid-go"
)

var ErrNotFound = errors.New("not found")

// TableWriter is the interface that defines methods for writing, updating, or deleting an entity from a DynamoDB table
// based on an opinionated implementation of DynamoDB by Table.
type TableWriter[T any] interface {
	WriteEntity(ctx context.Context, entity T) error
	UpdateEntity(ctx context.Context, entity T) error
	UpdateEntityVersion(ctx context.Context, oldVersion T, newVersion T) error
	DeleteEntity(ctx context.Context, entity T) error
	DeleteEntityWithID(ctx context.Context, entityID string) (T, error)
}

// TableReader is the interface that defines methods for reading entity-related information from a DynamoDB table based
// on an opinionated implementation of DynamoDB by Table.
type TableReader[T any] interface {
	GetRow(rowName string) TableRow[T]
	GetEntityRow() TableRow[T]
	ReadAllPartKeyValues(ctx context.Context, row TableRow[T]) ([]string, error)
	ReadAllSortKeyValues(ctx context.Context, row TableRow[T], partKeyValue string) ([]string, error)
	ReadPartKeyValues(ctx context.Context, row TableRow[T], reverse bool, limit int, offset string) ([]string, error)
	ReadSortKeyValues(ctx context.Context, row TableRow[T], partKeyValue string, reverse bool, limit int, offset string) ([]string, error)
	ReadFirstSortKeyID(ctx context.Context, row TableRow[T], partKeyValue string) (string, error)
	ReadLastSortKeyID(ctx context.Context, row TableRow[T], partKeyValue string) (string, error)
	ReadAllEntityIDs(ctx context.Context) ([]string, error)
	ReadEntityIDs(ctx context.Context, reverse bool, limit int, offset string) ([]string, error)
	ReadAllEntityVersionIDs(ctx context.Context, entityID string) ([]string, error)
	ReadEntityVersionIDs(ctx context.Context, entityID string, reverse bool, limit int, offset string) ([]string, error)
	ReadCurrentEntityVersionID(ctx context.Context, entityID string) (string, error)
	EntityExists(ctx context.Context, entityID string) bool
	EntityVersionExists(ctx context.Context, entityID string, versionID string) bool
	ReadEntities(ctx context.Context, entityIDs []string) []T
	ReadEntitiesAsJson(ctx context.Context, entityIDs []string) []byte
	ReadEntity(ctx context.Context, entityID string) (T, error)
	ReadEntityAsJson(ctx context.Context, entityID string) ([]byte, error)
	ReadEntityAsCompressedJson(ctx context.Context, entityID string) ([]byte, error)
	ReadEntityVersion(ctx context.Context, entityID string, versionID string) (T, error)
	ReadEntityVersionAsJson(ctx context.Context, entityID string, versionID string) ([]byte, error)
	ReadEntityVersions(ctx context.Context, entityID string, reverse bool, limit int, offset string) ([]T, error)
	ReadEntityVersionsAsJson(ctx context.Context, entityID string, reverse bool, limit int, offset string) ([]byte, error)
	ReadAllEntityVersions(ctx context.Context, entityID string) ([]T, error)
	ReadAllEntityVersionsAsJson(ctx context.Context, entityID string) ([]byte, error)
	ReadEntityFromRow(ctx context.Context, row TableRow[T], partKeyValue string, sortKeyValue string) (T, error)
	ReadEntityFromRowAsJson(ctx context.Context, row TableRow[T], partKeyValue string, sortKeyValue string) ([]byte, error)
	ReadEntityFromRowAsCompressedJson(ctx context.Context, row TableRow[T], partKeyValue string, sortKeyValue string) ([]byte, error)
	ReadEntitiesFromRow(ctx context.Context, row TableRow[T], partKeyValue string, reverse bool, limit int, offset string) ([]T, error)
	ReadEntitiesFromRowAsJson(ctx context.Context, row TableRow[T], partKeyValue string, reverse bool, limit int, offset string) ([]byte, error)
	ReadAllEntitiesFromRow(ctx context.Context, row TableRow[T], partKeyValue string) ([]T, error)
	ReadAllEntitiesFromRowAsJson(ctx context.Context, row TableRow[T], partKeyValue string) ([]byte, error)
	ReadTextValue(ctx context.Context, row TableRow[T], partKeyValue string, sortKeyValue string) (TextValue, error)
	ReadTextValues(ctx context.Context, row TableRow[T], partKeyValue string, reverse bool, limit int, offset string) ([]TextValue, error)
	ReadAllTextValues(ctx context.Context, row TableRow[T], partKeyValue string, sortByValue bool) ([]TextValue, error)
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
// The following attributes are stored in the table:
//  - v_part: partition key (required)
//  - v_sort: sorting key (required; for an entity without versions, reuse the partition key)
//  - v_json: gzip compressed JSON data (optional)
//  - v_text: string value (optional)
//  - v_num: double-wide floating point numeric value (optional)
//  - v_expires: expiration timestamp in epoch seconds (optional)
//
// Pipe-separated key values are used to avoid naming collisions, supporting multiple row types in a single table:
// Entity Values: rowName|partKeyName|partKeyValue -- (sortKeyValue, value), (sortKeyValue, value), ...
// Partition Keys: rowName|partKeyName -- (partKeyValue, count), (partKeyValue, count), ...
// Note that in addition to storing the entity data in a given row, we're also storing the partition key values used
// for each row, to support queries that "walk" the entire data set.
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
		rKey := Record{
			PartKeyValue: row.getRowPartKey(), // rowName|partKeyName
			SortKeyValue: partKeyValue,
		}
		if row.TimeToLive != nil {
			rKey.TimeToLive = row.TimeToLive(entity)
		}
		records = append(records, rKey)
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
// entity data are stored in a single table, reusing the attribute names indicated in the TableRow definition.
// The EntityRow is special; it contains the revision history for the entity, whereas the IndexRows contain only
// values from the latest revision of the entity.
type Table[T any] struct {
	Client     *dynamodb.Client
	EntityType string
	TableName  string
	TTL        bool
	EntityRow  TableRow[T]
	IndexRows  map[string]TableRow[T]
}

// GetRow returns the specified row definition.
func (table Table[T]) GetRow(rowName string) TableRow[T] {
	if rowName == "EntityRow" {
		return table.EntityRow
	}
	return table.IndexRows[rowName]
}

// GetEntityRow returns the entity row definition. This row contains the revision history for the entity.
func (table Table[T]) GetEntityRow() TableRow[T] {
	return table.EntityRow
}

// GetTableDescription fetches metadata about the DynamoDB table.
func (table Table[T]) GetTableDescription(ctx context.Context) (*dynamodb.DescribeTableOutput, error) {
	return table.Client.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(table.TableName),
	})
}

// TableExists returns true if the DynamoDB table exists.
func (table Table[T]) TableExists(ctx context.Context) bool {
	startTime := time.Now()
	desc, err := table.GetTableDescription(ctx)
	if err != nil {
		// Table Not Found is an expected error
		var nf *types.ResourceNotFoundException
		if errors.As(err, &nf) {
			log.Printf("table %s %s", table.TableName, nf.ErrorCode())
			return false
		}
		// Identify other possible errors
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
	log.Printf("table %s %s in %s\n", *desc.Table.TableName, desc.Table.TableStatus, time.Since(startTime))
	return true
}

// CreateTable creates the DynamoDB table for the entity.
// This operation may take a few seconds to complete, as it waits for the table to become active.
func (table Table[T]) CreateTable(ctx context.Context) error {
	startTime := time.Now()
	partKeyAttr := types.AttributeDefinition{
		AttributeName: aws.String("v_part"),
		AttributeType: types.ScalarAttributeTypeS,
	}
	sortKeyAttr := types.AttributeDefinition{
		AttributeName: aws.String("v_sort"),
		AttributeType: types.ScalarAttributeTypeS,
	}
	partKeyDef := types.KeySchemaElement{
		AttributeName: aws.String("v_part"),
		KeyType:       types.KeyTypeHash,
	}
	sortKeyDef := types.KeySchemaElement{
		AttributeName: aws.String("v_sort"),
		KeyType:       types.KeyTypeRange,
	}
	req := dynamodb.CreateTableInput{
		TableName:            aws.String(table.TableName),
		TableClass:           types.TableClassStandard,
		BillingMode:          types.BillingModePayPerRequest,
		AttributeDefinitions: []types.AttributeDefinition{partKeyAttr, sortKeyAttr},
		KeySchema:            []types.KeySchemaElement{partKeyDef, sortKeyDef},
	}
	res, err := table.Client.CreateTable(ctx, &req)
	if err != nil {
		return err
	}
	log.Printf("table %s %s in %s\n", *res.TableDescription.TableName, res.TableDescription.TableStatus, time.Since(startTime))

	// Use a Waiter to detect the transition from CREATING to ACTIVE:
	// https://aws.github.io/aws-sdk-go-v2/docs/making-requests/#using-waiters
	waiter := dynamodb.NewTableExistsWaiter(table.Client, func(o *dynamodb.TableExistsWaiterOptions) {
		o.MinDelay = 3 * time.Second   // default is 20 seconds
		o.MaxDelay = 120 * time.Second // default is 120 seconds
	})
	desc, err := waiter.WaitForOutput(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(table.TableName)}, 3*time.Minute)
	if err != nil {
		return err
	}
	log.Printf("table %s %s in %s\n", *desc.Table.TableName, desc.Table.TableStatus, time.Since(startTime))

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
	startTime := time.Now()
	req := dynamodb.UpdateTimeToLiveInput{
		TableName: aws.String(table.TableName),
		TimeToLiveSpecification: &types.TimeToLiveSpecification{
			AttributeName: aws.String("v_expires"),
			Enabled:       aws.Bool(table.TTL),
		},
	}
	res, err := table.Client.UpdateTimeToLive(ctx, &req)
	if err != nil {
		return err
	}
	log.Printf("table %s TTL set to %s %t in %s\n", table.TableName, *res.TimeToLiveSpecification.AttributeName,
		*res.TimeToLiveSpecification.Enabled, time.Since(startTime))
	return nil
}

// DeleteTable deletes the DynamoDB table. Note that the table's status may be DELETING for a few seconds.
func (table Table[T]) DeleteTable(ctx context.Context) error {
	startTime := time.Now()
	req := dynamodb.DeleteTableInput{
		TableName: aws.String(table.TableName),
	}
	res, err := table.Client.DeleteTable(ctx, &req)
	if err != nil {
		return err
	}
	log.Printf("table %s %s in %s\n", *res.TableDescription.TableName, res.TableDescription.TableStatus,
		time.Since(startTime))
	return nil
}

// writeBatches bulk-writes the provided WriteRequests to the table, respecting DynamoDB limits:
// 25 write requests per batch, 16MB per batch, and 400KB per item. For more information, see:
// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Limits.html
func (table Table[T]) writeBatches(ctx context.Context, writeRequests []types.WriteRequest) error {
	batches := MakeBatches(writeRequests, 25)
	for _, batch := range batches {
		requestItems := map[string][]types.WriteRequest{
			table.TableName: batch,
		}
		for len(requestItems) > 0 {
			// Create and send a batch write request
			req := dynamodb.BatchWriteItemInput{RequestItems: requestItems}
			res, err := table.Client.BatchWriteItem(ctx, &req)
			if err != nil {
				return err
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
	writeRequests := Map(records, func(r Record) types.WriteRequest { return r.PutRequest() })
	return table.writeBatches(ctx, writeRequests)
}

// UpdateEntity batch-writes the provided entity to each of the wide rows in the table,
// including deleting any obsolete values in index rows.
func (table Table[T]) UpdateEntity(ctx context.Context, entity T) error {
	// Fetch the current (obsolete) version of the entity
	entityID := table.EntityRow.PartKeyValue(entity)
	oldVersion, err := table.ReadEntity(ctx, entityID)
	if err != nil {
		return err
	}
	// Update the entity with the new version
	return table.UpdateEntityVersion(ctx, oldVersion, entity)
}

// UpdateEntityVersion batch-writes the provided entity to each of the wide rows in the table,
// including deleting any obsolete values in index rows.
func (table Table[T]) UpdateEntityVersion(ctx context.Context, oldVersion T, newVersion T) error {
	// Generate deletion requests for deleted index row partition keys
	var deleteRecords []Record
	if table.IndexRows != nil {
		for _, row := range table.IndexRows {
			deletedKeys := row.getDeletedPartKeyValues(oldVersion, newVersion)
			sortKeyValue := row.SortKeyValue(oldVersion)
			deleteRecords = append(deleteRecords, row.getDeleteRecordsForKeys(deletedKeys, sortKeyValue)...)
		}
	}
	writeRequests := Map(deleteRecords, func(r Record) types.WriteRequest { return r.DeleteRequest() })
	// Generate put requests for the new version of the entity
	writeRecords := table.EntityRow.getWriteRecords(newVersion)
	if table.IndexRows != nil {
		for _, row := range table.IndexRows {
			writeRecords = append(writeRecords, row.getWriteRecords(newVersion)...)
		}
	}
	for _, record := range writeRecords {
		writeRequests = append(writeRequests, record.PutRequest())
	}
	// Write batches, respecting the specified maximum batch size
	return table.writeBatches(ctx, writeRequests)
}

// DeleteEntity deletes the provided entity from the table (including all versions).
func (table Table[T]) DeleteEntity(ctx context.Context, entity T) error {
	entityID := table.EntityRow.PartKeyValue(entity)
	// Delete index row items based on the current version of the entity
	var deleteRecords []Record
	if table.IndexRows != nil {
		for _, row := range table.IndexRows {
			deleteRecords = append(deleteRecords, row.getDeleteRecords(entity)...)
		}
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
		return err
	}
	for _, sortKey := range sortKeys {
		deleteRecords = append(deleteRecords, Record{
			PartKeyValue: partKey,
			SortKeyValue: sortKey,
		})
	}
	// Write the batch
	writeRequests := Map(deleteRecords, func(r Record) types.WriteRequest { return r.DeleteRequest() })
	return table.writeBatches(ctx, writeRequests)
}

// DeleteEntityWithID deletes the specified entity from the table (including all versions).
func (table Table[T]) DeleteEntityWithID(ctx context.Context, entityID string) (T, error) {
	entity, err := table.ReadEntity(ctx, entityID)
	if err != nil {
		return entity, err
	}
	return entity, table.DeleteEntity(ctx, entity)
}

// DeleteRow deletes the row for the specified partition key.
func (table Table[T]) DeleteRow(ctx context.Context, row TableRow[T], partKeyValue string) error {
	if partKeyValue == "" {
		return errors.New("partition key value must not be empty")
	}
	partKey := row.getRowPartKeyValue(partKeyValue)
	sortKeys, err := table.ReadSortKeyValues(ctx, row, partKeyValue, false, 250, "0")
	if err != nil {
		return err
	}
	for len(sortKeys) > 0 {
		var requests []types.WriteRequest
		for _, sortKey := range sortKeys {
			item := map[string]types.AttributeValue{
				"v_part": &types.AttributeValueMemberS{Value: partKey},
				"v_sort": &types.AttributeValueMemberS{Value: sortKey},
			}
			request := types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: item}}
			requests = append(requests, request)
		}
		err := table.writeBatches(ctx, requests)
		if err != nil {
			return err
		}
		offset := sortKeys[len(sortKeys)-1]
		sortKeys, err = table.ReadSortKeyValues(ctx, row, partKeyValue, false, 250, offset)
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteItem deletes the item with the specified row partition key and sort key.
// Note that this method will seldom be used, perhaps for cleaning up obsolete items.
func (table Table[T]) DeleteItem(ctx context.Context, rowPartKeyValue string, sortKeyValue string) error {
	if rowPartKeyValue == "" || sortKeyValue == "" {
		return errors.New("rowPartKeyValue and sortKeyValue must not be empty")
	}
	item := map[string]types.AttributeValue{
		"v_part": &types.AttributeValueMemberS{Value: rowPartKeyValue},
		"v_sort": &types.AttributeValueMemberS{Value: sortKeyValue},
	}
	req := dynamodb.DeleteItemInput{
		TableName: aws.String(table.TableName),
		Key:       item,
	}
	_, err := table.Client.DeleteItem(ctx, &req)
	return err // usually nil
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
			"#p": "v_part",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: partKey},
		},
		KeyConditionExpression: aws.String("#p = :p"),
		ProjectionExpression:   aws.String("v_sort"),
		ScanIndexForward:       aws.Bool(true),
	}
	paginator := dynamodb.NewQueryPaginator(table.Client, &req)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return keyValues, err
		}
		for _, item := range page.Items {
			if item == nil || item["v_sort"] == nil {
				continue
			}
			attrValue := item["v_sort"]
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
	var keyName string
	if partKeyValue == "" {
		// Read partition key values
		keyName = row.getRowPartKey()
	} else {
		// Read sort key values
		keyName = row.getRowPartKeyValue(partKeyValue)
	}
	if offset == "" {
		// An empty offset means start from the beginning
		offset = "0"
	}
	keyValues := make([]string, 0)
	keyConditionExpression := "#p = :p and #s > :s"
	if reverse {
		keyConditionExpression = "#p = :p and #s < :s"
	}
	req := dynamodb.QueryInput{
		TableName: aws.String(table.TableName),
		ExpressionAttributeNames: map[string]string{
			"#p": "v_part",
			"#s": "v_sort",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: keyName},
			":s": &types.AttributeValueMemberS{Value: offset},
		},
		KeyConditionExpression: aws.String(keyConditionExpression),
		ProjectionExpression:   aws.String("v_sort"),
		ScanIndexForward:       aws.Bool(!reverse),
		Limit:                  aws.Int32(int32(limit)),
	}
	res, err := table.Client.Query(ctx, &req)
	if err != nil {
		return keyValues, err
	}
	for _, item := range res.Items {
		if item == nil || item["v_sort"] == nil {
			continue
		}
		attrValue := item["v_sort"]
		keyValue := attrValue.(*types.AttributeValueMemberS).Value
		if keyValue != "" {
			keyValues = append(keyValues, keyValue)
		}
	}
	return keyValues, nil
}

// ReadFirstSortKeyID reads the first sort key value for the specified row and partition key value.
// This method is typically used for looking up an ID corresponding to a foreign key.
func (table Table[T]) ReadFirstSortKeyID(ctx context.Context, row TableRow[T], partKeyValue string) (string, error) {
	keyValues, err := table.ReadSortKeyValues(ctx, row, partKeyValue, false, 1, tuid.MinID)
	if err != nil || len(keyValues) == 0 {
		return "", err
	}
	return keyValues[0], nil
}

// ReadLastSortKeyID reads the last sort key value for the specified row and partition key value.
// This method is typically used for looking up the current version ID of a versioned entity.
func (table Table[T]) ReadLastSortKeyID(ctx context.Context, row TableRow[T], partKeyValue string) (string, error) {
	keyValues, err := table.ReadSortKeyValues(ctx, row, partKeyValue, true, 1, tuid.MaxID)
	if err != nil || len(keyValues) == 0 {
		return "", err
	}
	return keyValues[0], nil
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
	return table.ReadLastSortKeyID(ctx, table.EntityRow, entityID)
}

// EntityExists checks if an entity exists in the table.
func (table Table[T]) EntityExists(ctx context.Context, entityID string) bool {
	versionID, err := table.ReadFirstSortKeyID(ctx, table.EntityRow, entityID)
	return err == nil && versionID != ""
}

// EntityVersionExists checks if an entity version exists in the table.
func (table Table[T]) EntityVersionExists(ctx context.Context, entityID string, versionID string) bool {
	req := dynamodb.GetItemInput{
		TableName: aws.String(table.TableName),
		Key: map[string]types.AttributeValue{
			"v_part": &types.AttributeValueMemberS{Value: table.EntityRow.getRowPartKeyValue(entityID)},
			"v_sort": &types.AttributeValueMemberS{Value: versionID},
		},
		ProjectionExpression: aws.String("v_part,v_sort"),
	}
	res, err := table.Client.GetItem(ctx, &req)
	return err == nil && res.Item != nil && res.Item["v_part"] != nil && res.Item["v_sort"] != nil
}

// ReadEntities reads the current versions of the specified entities from the EntityRow.
// If an entity does not exist, it will be omitted from the results.
func (table Table[T]) ReadEntities(ctx context.Context, entityIDs []string) []T {
	entities := make([]T, 0)
	var batches [][]string
	maxBatchSize := 10 // concurrent request limit
	batches = MakeBatches(entityIDs, maxBatchSize)
	for _, batch := range batches {
		batchSize := len(batch)
		results := make(chan T, batchSize)
		var wg sync.WaitGroup
		wg.Add(batchSize)
		for _, entityID := range batch {
			go func(entityID string) {
				defer wg.Done()
				entity, err := table.ReadEntity(ctx, entityID)
				if err != nil {
					// log the problem but continue
				} else {
					results <- entity
				}
			}(entityID)
		}
		wg.Wait()
		close(results)
		for entity := range results {
			entities = append(entities, entity)
		}
	}
	return entities
}

// ReadEntitiesAsJson reads the current versions of the specified entities from the EntityRow as a JSON byte slice.
// If an entity does not exist, it will be omitted from the results.
func (table Table[T]) ReadEntitiesAsJson(ctx context.Context, entityIDs []string) []byte {
	var entities bytes.Buffer
	entities.WriteString("[")
	var i int
	var batches [][]string
	maxBatchSize := 10 // concurrent request limit
	batches = MakeBatches(entityIDs, maxBatchSize)
	for _, batch := range batches {
		batchSize := len(batch)
		results := make(chan []byte, batchSize)
		var wg sync.WaitGroup
		wg.Add(batchSize)
		for _, entityID := range batch {
			go func(entityID string) {
				defer wg.Done()
				entity, err := table.ReadEntityAsJson(ctx, entityID)
				if err != nil {
					// log the problem but continue
				} else {
					results <- entity
				}
			}(entityID)
		}
		wg.Wait()
		close(results)
		for entity := range results {
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
func (table Table[T]) ReadEntity(ctx context.Context, entityID string) (T, error) {
	var entity T
	gzJson, err := table.ReadEntityAsCompressedJson(ctx, entityID)
	if err != nil {
		return entity, err
	}
	err = FromCompressedJson(gzJson, &entity)
	if err != nil {
		return entity, err
	}
	return entity, nil
}

// ReadEntityAsJson reads the current version of the specified entity as a JSON byte slice.
func (table Table[T]) ReadEntityAsJson(ctx context.Context, entityID string) ([]byte, error) {
	gzJson, err := table.ReadEntityAsCompressedJson(ctx, entityID)
	if err != nil {
		return nil, err
	}
	return UncompressJson(gzJson)
}

// ReadEntityAsCompressedJson reads the current version of the specified entity, returning a compressed JSON byte slice.
func (table Table[T]) ReadEntityAsCompressedJson(ctx context.Context, entityID string) ([]byte, error) {
	if table.EntityRow.JsonValue == nil {
		return nil, errors.New("the EntityRow must contain compressed JSON values")
	}
	if entityID == "" {
		return nil, errors.New("the entity ID must be provided")
	}
	// Read the current version of a versioned entity
	req := dynamodb.QueryInput{
		TableName: aws.String(table.TableName),
		ExpressionAttributeNames: map[string]string{
			"#p": "v_part",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: table.EntityRow.getRowPartKeyValue(entityID)},
		},
		KeyConditionExpression: aws.String("#p = :p"),
		ProjectionExpression:   aws.String("v_json"),
		ScanIndexForward:       aws.Bool(false),
		Limit:                  aws.Int32(1),
	}
	res, err := table.Client.Query(ctx, &req)
	if err != nil {
		return nil, err
	}
	if len(res.Items) == 0 || res.Items[0]["v_json"] == nil {
		return nil, ErrNotFound
	}
	return res.Items[0]["v_json"].(*types.AttributeValueMemberB).Value, nil
}

// ReadEntityVersion reads the specified version of the specified entity.
func (table Table[T]) ReadEntityVersion(ctx context.Context, entityID string, versionID string) (T, error) {
	return table.ReadEntityFromRow(ctx, table.EntityRow, entityID, versionID)
}

// ReadEntityVersionAsJson reads the specified version of the specified entity as a JSON byte slice.
func (table Table[T]) ReadEntityVersionAsJson(ctx context.Context, entityID string, versionID string) ([]byte, error) {
	return table.ReadEntityFromRowAsJson(ctx, table.EntityRow, entityID, versionID)
}

// ReadEntityVersions reads paginated versions of the specified entity.
func (table Table[T]) ReadEntityVersions(ctx context.Context, entityID string, reverse bool, limit int, offset string) ([]T, error) {
	return table.ReadEntitiesFromRow(ctx, table.EntityRow, entityID, reverse, limit, offset)
}

// ReadEntityVersionsAsJson reads paginated versions of the specified entity as a JSON byte slice.
func (table Table[T]) ReadEntityVersionsAsJson(ctx context.Context, entityID string, reverse bool, limit int, offset string) ([]byte, error) {
	return table.ReadEntitiesFromRowAsJson(ctx, table.EntityRow, entityID, reverse, limit, offset)
}

// ReadAllEntityVersions reads all versions of the specified entity.
// Note: this can return a very large number of versions! Use with caution.
func (table Table[T]) ReadAllEntityVersions(ctx context.Context, entityID string) ([]T, error) {
	return table.ReadAllEntitiesFromRow(ctx, table.EntityRow, entityID)
}

// ReadAllEntityVersionsAsJson reads all versions of the specified entity as a JSON byte slice.
// Note: this can return a very large number of versions! Use with caution.
func (table Table[T]) ReadAllEntityVersionsAsJson(ctx context.Context, entityID string) ([]byte, error) {
	return table.ReadAllEntitiesFromRowAsJson(ctx, table.EntityRow, entityID)
}

// ReadEntityFromRow reads the specified entity from the specified row.
func (table Table[T]) ReadEntityFromRow(ctx context.Context, row TableRow[T], partKeyValue string, sortKeyValue string) (T, error) {
	var entity T
	gzJson, err := table.ReadEntityFromRowAsCompressedJson(ctx, row, partKeyValue, sortKeyValue)
	if err != nil {
		return entity, err
	}
	err = FromCompressedJson(gzJson, &entity)
	if err != nil {
		return entity, err
	}
	return entity, nil
}

// ReadEntityFromRowAsJson reads the specified entity from the specified row as JSON bytes.
func (table Table[T]) ReadEntityFromRowAsJson(ctx context.Context, row TableRow[T], partKeyValue string, sortKeyValue string) ([]byte, error) {
	gzJson, err := table.ReadEntityFromRowAsCompressedJson(ctx, row, partKeyValue, sortKeyValue)
	if err != nil {
		return nil, err
	}
	return UncompressJson(gzJson)
}

// ReadEntityFromRowAsCompressedJson reads the specified entity from the specified row as compressed JSON bytes.
func (table Table[T]) ReadEntityFromRowAsCompressedJson(ctx context.Context, row TableRow[T], partKeyValue string, sortKeyValue string) ([]byte, error) {
	if row.JsonValue == nil {
		return nil, errors.New("row " + row.RowName + " must contain compressed JSON values")
	}
	req := dynamodb.GetItemInput{
		TableName: aws.String(table.TableName),
		Key: map[string]types.AttributeValue{
			"v_part": &types.AttributeValueMemberS{Value: row.getRowPartKeyValue(partKeyValue)},
			"v_sort": &types.AttributeValueMemberS{Value: sortKeyValue},
		},
		ProjectionExpression: aws.String("v_json"),
	}
	res, err := table.Client.GetItem(ctx, &req)
	if err != nil {
		return nil, err
	}
	if res.Item == nil || res.Item["v_json"] == nil {
		return nil, ErrNotFound
	}
	return res.Item["v_json"].(*types.AttributeValueMemberB).Value, nil
}

// ReadEntitiesFromRow reads paginated entities from the specified row.
func (table Table[T]) ReadEntitiesFromRow(ctx context.Context, row TableRow[T], partKeyValue string, reverse bool, limit int, offset string) ([]T, error) {
	entities := make([]T, 0)
	if row.JsonValue == nil {
		return entities, errors.New("row " + row.RowName + " must contain compressed JSON values")
	}
	keyConditionExpression := "#p = :p and #s > :s"
	if reverse {
		keyConditionExpression = "#p = :p and #s < :s"
	}
	req := dynamodb.QueryInput{
		TableName: aws.String(table.TableName),
		ExpressionAttributeNames: map[string]string{
			"#p": "v_part",
			"#s": "v_sort",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: row.getRowPartKeyValue(partKeyValue)},
			":s": &types.AttributeValueMemberS{Value: offset},
		},
		KeyConditionExpression: aws.String(keyConditionExpression),
		ProjectionExpression:   aws.String("v_json"),
		ScanIndexForward:       aws.Bool(!reverse),
		Limit:                  aws.Int32(int32(limit)),
	}
	res, err := table.Client.Query(ctx, &req)
	if err != nil {
		return entities, err
	}
	for _, item := range res.Items {
		if item == nil || item["v_json"] == nil {
			continue
		}
		gzJson := item["v_json"].(*types.AttributeValueMemberB).Value
		var entity T
		err = FromCompressedJson(gzJson, &entity)
		if err != nil {
			return entities, err
		}
		entities = append(entities, entity)
	}
	return entities, nil
}

// ReadEntitiesFromRowAsJson reads paginated entities from the specified row, returning a JSON byte slice.
func (table Table[T]) ReadEntitiesFromRowAsJson(ctx context.Context, row TableRow[T], partKeyValue string, reverse bool, limit int, offset string) ([]byte, error) {
	if row.JsonValue == nil {
		return nil, errors.New("row " + row.RowName + " must contain compressed JSON values")
	}
	keyConditionExpression := "#p = :p and #s > :s"
	if reverse {
		keyConditionExpression = "#p = :p and #s < :s"
	}
	req := dynamodb.QueryInput{
		TableName: aws.String(table.TableName),
		ExpressionAttributeNames: map[string]string{
			"#p": "v_part",
			"#s": "v_sort",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: row.getRowPartKeyValue(partKeyValue)},
			":s": &types.AttributeValueMemberS{Value: offset},
		},
		KeyConditionExpression: aws.String(keyConditionExpression),
		ProjectionExpression:   aws.String("v_json"),
		ScanIndexForward:       aws.Bool(!reverse),
		Limit:                  aws.Int32(int32(limit)),
	}
	res, err := table.Client.Query(ctx, &req)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	buf.WriteString("[")
	for i, item := range res.Items {
		if item == nil || item["v_json"] == nil {
			continue
		}
		gzJson := item["v_json"].(*types.AttributeValueMemberB).Value
		jsonBytes, err := UncompressJson(gzJson)
		if err != nil {
			continue
		}
		if i > 0 {
			buf.WriteString(",")
		}
		buf.Write(jsonBytes)
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
	req := dynamodb.QueryInput{
		TableName: aws.String(table.TableName),
		ExpressionAttributeNames: map[string]string{
			"#p": "v_part",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: row.getRowPartKeyValue(partKeyValue)},
		},
		KeyConditionExpression: aws.String("#p = :p"),
		ProjectionExpression:   aws.String("v_json"),
		ScanIndexForward:       aws.Bool(true),
	}
	paginator := dynamodb.NewQueryPaginator(table.Client, &req)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return entities, err
		}
		for _, item := range page.Items {
			if item == nil || item["v_json"] == nil {
				continue
			}
			gzJson := item["v_json"].(*types.AttributeValueMemberB).Value
			var entity T
			err = FromCompressedJson(gzJson, &entity)
			if err != nil {
				return entities, err
			}
			entities = append(entities, entity)
		}
	}
	return entities, nil
}

// ReadAllEntitiesFromRowAsJson reads all entities from the specified row as a JSON byte slice.
// Note: this could be a lot of data! It should only be used for small collections.
func (table Table[T]) ReadAllEntitiesFromRowAsJson(ctx context.Context, row TableRow[T], partKeyValue string) ([]byte, error) {
	if row.JsonValue == nil {
		return nil, errors.New("row " + row.RowName + " must contain compressed JSON values")
	}
	req := dynamodb.QueryInput{
		TableName: aws.String(table.TableName),
		ExpressionAttributeNames: map[string]string{
			"#p": "v_part",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: row.getRowPartKeyValue(partKeyValue)},
		},
		KeyConditionExpression: aws.String("#p = :p"),
		ProjectionExpression:   aws.String("v_json"),
		ScanIndexForward:       aws.Bool(true),
	}
	paginator := dynamodb.NewQueryPaginator(table.Client, &req)
	var comma bool
	var buf bytes.Buffer
	buf.WriteString("[")
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, item := range page.Items {
			if item == nil || item["v_json"] == nil {
				continue
			}
			gzJson := item["v_json"].(*types.AttributeValueMemberB).Value
			jsonBytes, err := UncompressJson(gzJson)
			if err != nil {
				continue
			}
			if comma {
				buf.WriteString(",\n")
			}
			buf.Write(jsonBytes)
			comma = true
		}
	}
	buf.WriteString("]")
	return buf.Bytes(), nil
}

// ReadTextValue reads the specified text value from the specified row.
func (table Table[T]) ReadTextValue(ctx context.Context, row TableRow[T], partKeyValue string, sortKeyValue string) (TextValue, error) {
	var textValue TextValue
	if row.TextValue == nil {
		return textValue, errors.New("row " + row.RowName + " must contain text values")
	}
	req := dynamodb.GetItemInput{
		TableName: aws.String(table.TableName),
		Key: map[string]types.AttributeValue{
			"v_part": &types.AttributeValueMemberS{Value: row.getRowPartKeyValue(partKeyValue)},
			"v_sort": &types.AttributeValueMemberS{Value: sortKeyValue},
		},
		ProjectionExpression: aws.String("v_sort, v_text"),
	}
	res, err := table.Client.GetItem(ctx, &req)
	if err != nil {
		return textValue, err
	}
	if res.Item == nil || res.Item["v_sort"] == nil || res.Item["v_text"] == nil {
		return textValue, ErrNotFound
	}
	textValue.Key = res.Item["v_sort"].(*types.AttributeValueMemberS).Value
	textValue.Value = res.Item["v_text"].(*types.AttributeValueMemberS).Value
	return textValue, nil
}

// ReadTextValues reads paginated text values from the specified row.
func (table Table[T]) ReadTextValues(ctx context.Context, row TableRow[T], partKeyValue string, reverse bool, limit int, offset string) ([]TextValue, error) {
	textValues := make([]TextValue, 0)
	if row.TextValue == nil {
		return textValues, errors.New("row " + row.RowName + " must contain text values")
	}
	keyConditionExpression := "#p = :p and #s > :s"
	if reverse {
		keyConditionExpression = "#p = :p and #s < :s"
	}
	req := dynamodb.QueryInput{
		TableName: aws.String(table.TableName),
		ExpressionAttributeNames: map[string]string{
			"#p": "v_part",
			"#s": "v_sort",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: row.getRowPartKeyValue(partKeyValue)},
			":s": &types.AttributeValueMemberS{Value: offset},
		},
		KeyConditionExpression: aws.String(keyConditionExpression),
		ProjectionExpression:   aws.String("v_sort, v_text"),
		ScanIndexForward:       aws.Bool(!reverse),
		Limit:                  aws.Int32(int32(limit)),
	}
	res, err := table.Client.Query(ctx, &req)
	if err != nil {
		return textValues, err
	}
	for _, item := range res.Items {
		if item == nil || item["v_sort"] == nil || item["v_text"] == nil {
			continue
		}
		key := item["v_sort"].(*types.AttributeValueMemberS).Value
		value := item["v_text"].(*types.AttributeValueMemberS).Value
		textValues = append(textValues, TextValue{Key: key, Value: value})
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
	req := dynamodb.QueryInput{
		TableName: aws.String(table.TableName),
		ExpressionAttributeNames: map[string]string{
			"#p": "v_part",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: row.getRowPartKeyValue(partKeyValue)},
		},
		KeyConditionExpression: aws.String("#p = :p"),
		ProjectionExpression:   aws.String("v_sort, v_text"),
		ScanIndexForward:       aws.Bool(true),
	}
	paginator := dynamodb.NewQueryPaginator(table.Client, &req)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return textValues, err
		}
		for _, item := range page.Items {
			if item == nil || item["v_sort"] == nil || item["v_text"] == nil {
				continue
			}
			key := item["v_sort"].(*types.AttributeValueMemberS).Value
			value := item["v_text"].(*types.AttributeValueMemberS).Value
			textValues = append(textValues, TextValue{Key: key, Value: value})
		}
	}
	if sortByValue {
		sort.Slice(textValues, func(i, j int) bool {
			return textValues[i].Value < textValues[j].Value
		})
	}
	return textValues, nil
}

// ReadNumericValue reads the specified numeric value from the specified row.
func (table Table[T]) ReadNumericValue(ctx context.Context, row TableRow[T], partKeyValue string, sortKeyValue string) (NumValue, error) {
	var numValue NumValue
	if row.NumericValue == nil {
		return numValue, errors.New("row " + row.RowName + " must contain numeric values")
	}
	req := dynamodb.GetItemInput{
		TableName: aws.String(table.TableName),
		Key: map[string]types.AttributeValue{
			"v_part": &types.AttributeValueMemberS{Value: row.getRowPartKeyValue(partKeyValue)},
			"v_sort": &types.AttributeValueMemberS{Value: sortKeyValue},
		},
		ProjectionExpression: aws.String("v_sort, v_num"),
	}
	res, err := table.Client.GetItem(ctx, &req)
	if err != nil {
		return numValue, err
	}
	if res.Item == nil || res.Item["v_sort"] == nil || res.Item["v_num"] == nil {
		return numValue, ErrNotFound
	}
	numValue.Key = res.Item["v_sort"].(*types.AttributeValueMemberS).Value
	numValue.Value, _ = strconv.ParseFloat(res.Item["v_num"].(*types.AttributeValueMemberN).Value, 64)
	return numValue, nil
}

// ReadNumericValues reads paginated numeric values from the specified row.
func (table Table[T]) ReadNumericValues(ctx context.Context, row TableRow[T], partKeyValue string, reverse bool, limit int, offset string) ([]NumValue, error) {
	numValues := make([]NumValue, 0)
	if row.NumericValue == nil {
		return numValues, errors.New("row " + row.RowName + " must contain numeric values")
	}
	keyConditionExpression := "#p = :p and #s > :s"
	if reverse {
		keyConditionExpression = "#p = :p and #s < :s"
	}
	req := dynamodb.QueryInput{
		TableName: aws.String(table.TableName),
		ExpressionAttributeNames: map[string]string{
			"#p": "v_part",
			"#s": "v_sort",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: row.getRowPartKeyValue(partKeyValue)},
			":s": &types.AttributeValueMemberS{Value: offset},
		},
		KeyConditionExpression: aws.String(keyConditionExpression),
		ProjectionExpression:   aws.String("v_sort, v_num"),
		ScanIndexForward:       aws.Bool(!reverse),
		Limit:                  aws.Int32(int32(limit)),
	}
	res, err := table.Client.Query(ctx, &req)
	if err != nil {
		return numValues, err
	}
	for _, item := range res.Items {
		if item == nil || item["v_sort"] == nil || item["v_num"] == nil {
			continue
		}
		key := item["v_sort"].(*types.AttributeValueMemberS).Value
		value, _ := strconv.ParseFloat(item["v_num"].(*types.AttributeValueMemberN).Value, 64)
		numValues = append(numValues, NumValue{Key: key, Value: value})
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
	req := dynamodb.QueryInput{
		TableName: aws.String(table.TableName),
		ExpressionAttributeNames: map[string]string{
			"#p": "v_part",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":p": &types.AttributeValueMemberS{Value: row.getRowPartKeyValue(partKeyValue)},
		},
		KeyConditionExpression: aws.String("#p = :p"),
		ProjectionExpression:   aws.String("v_sort, v_num"),
		ScanIndexForward:       aws.Bool(true),
	}
	paginator := dynamodb.NewQueryPaginator(table.Client, &req)
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return numValues, err
		}
		for _, item := range page.Items {
			if item == nil || item["v_sort"] == nil || item["v_num"] == nil {
				continue
			}
			key := item["v_sort"].(*types.AttributeValueMemberS).Value
			value, _ := strconv.ParseFloat(item["v_num"].(*types.AttributeValueMemberN).Value, 64)
			numValues = append(numValues, NumValue{Key: key, Value: value})
		}
	}
	if sortByValue {
		sort.Slice(numValues, func(i, j int) bool {
			return numValues[i].Value < numValues[j].Value
		})
	}
	return numValues, nil
}

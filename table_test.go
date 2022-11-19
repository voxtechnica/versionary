package versionary

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/voxtechnica/tuid-go"
)

var (
	testTable TableReadWriter[versionableThing]
	ctx       = context.Background()
	v10       = newVersionableThing("zyx v1.0", 3, []string{"tag1", "tag2"})
	v11       = updatedVersionableThing(v10, "zyx v1.1", 2, []string{"tag1", "tag3"})
	v20       = newVersionableThing("abc v2.0", 1, []string{"tag1", "tag2"})
)

func TestMain(m *testing.M) {
	// Set up DynamoDB Client
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		fmt.Printf("unable to load SDK config, %v\n", err)
		os.Exit(1)
	}
	dbClient := dynamodb.NewFromConfig(cfg)

	// Set up Entity Table, creating it if needed
	table := newThingTable(dbClient, "test")
	if !table.IsValid() {
		fmt.Println("invalid DynamoDB table definition", table.TableName)
		os.Exit(1)
	}
	if !table.TableExists(ctx) {
		err = table.CreateTable(ctx)
		if err != nil {
			fmt.Printf("table %s creation failed: %v\n", table.TableName, err)
			os.Exit(1)
		}
	}
	memTable := NewMemTable(table)
	if !memTable.IsValid() {
		fmt.Println("invalid in-memory table definition", memTable.TableName)
		os.Exit(1)
	}

	// Run all the TableReader tests
	fmt.Println("Running the in-memory TableReader tests ...")
	initTestDatabase(ctx, memTable)
	testTable = memTable
	exitVal := m.Run()
	if exitVal != 0 {
		os.Exit(exitVal)
	}

	fmt.Println("Running the DynamoDB TableReader tests ...")
	initTestDatabase(ctx, table)
	testTable = table
	exitVal = m.Run()

	// Delete an entity
	err = table.DeleteEntity(ctx, v20)
	if err != nil {
		fmt.Printf("DynamoDB: v2.0 delete failed: %v\n", err)
	}
	err = memTable.DeleteEntity(ctx, v20)
	if err != nil {
		fmt.Printf("MemTable: v2.0 delete failed: %v\n", err)
	}

	// Delete an entity version
	err = deleteEntityVersion(table)
	if err != nil {
		fmt.Printf("DynamoDB: %v\n", err)
	}
	err = deleteEntityVersion(memTable)
	if err != nil {
		fmt.Printf("MemTable: %v\n", err)
	}

	// Delete the temporary testing table if it exists
	if table.TableExists(ctx) {
		err = table.DeleteTable(ctx)
		if err != nil {
			fmt.Printf("table %s deletion failed: %v\n", table.TableName, err)
		}
	}
	os.Exit(exitVal)
}

// deleteEntityVersion tests an entity version deletion for the specified table
func deleteEntityVersion(tbl TableReadWriter[versionableThing]) error {
	// Delete the current version
	v11Deleted, err := tbl.DeleteEntityVersionWithID(ctx, v11.ID, v11.UpdateID)
	if err != nil {
		return fmt.Errorf("v1.1 delete version failed: %v", err)
	}
	// Expect the result to be the deleted version
	if v11Deleted.Message != v11.Message {
		return fmt.Errorf("v1.1 delete version failed: expected %s, got %s", v11.Message, v11Deleted.Message)
	}
	// Expect the new current version to be the previous version
	v10Current, err := tbl.ReadEntity(ctx, v11.ID)
	if err != nil {
		return fmt.Errorf("v1.1 delete version read current failed: %v", err)
	}
	if v10Current.Message != v10.Message {
		return fmt.Errorf("v1.1 delete version read current failed: expected %s, got %s", v10.Message, v10Current.Message)
	}
	// Expect the index row to be rolled back to the previous version
	row, ok := tbl.GetRow("messages_tag")
	if !ok {
		return fmt.Errorf("v1.1 delete version failed: row messages_tag not found")
	}
	values, err := tbl.ReadAllTextValues(ctx, row, "tag2", true)
	if err != nil {
		return fmt.Errorf("v1.1 delete version read text values failed: %v", err)
	}
	if len(values) < 1 {
		return fmt.Errorf("v1.1 delete version read text values failed: expected value(s), got %d", len(values))
	}
	if values[len(values)-1].Value != v10.Message {
		return fmt.Errorf("v1.1 delete version read text values failed: expected %s, got %s", v10.Message, values[len(values)-1].Value)
	}
	return nil
}

func TestJSON(t *testing.T) {
	j, err := ToJSON(v10)
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}
	v10check, err := FromJSON[versionableThing](j)
	if err != nil {
		t.Errorf("FromJSON failed: %v", err)
	} else if !reflect.DeepEqual(v10, v10check) {
		t.Errorf("v10 and v10Check are not equal")
	}
}

func TestCompressedJSON(t *testing.T) {
	gzJSON, err := ToCompressedJSON(v11)
	if err != nil {
		t.Fatalf("ToCompressedJSON failed: %v", err)
	}
	v11Check, err := FromCompressedJSON[versionableThing](gzJSON)
	if err != nil {
		t.Errorf("FromCompressedJSON failed: %v", err)
	} else if !reflect.DeepEqual(v11, v11Check) {
		t.Errorf("v11 and v11Check are not equal")
	}
}

func TestTableRow_GetPartKeyValues(t *testing.T) {
	row, ok := testTable.GetRow("messages_tag")
	if !ok {
		t.Fatalf("messages_tag row not found")
	}
	if !row.IsValid() {
		t.Fatalf("messages_tag row is not valid")
	}
	values := row.getPartKeyValues(v10)
	if len(values) != 2 {
		t.Fatalf("expected 2 values, got %d", len(values))
	}
	added := row.getAddedPartKeyValues(v10, v11)
	if len(added) != 1 {
		t.Fatalf("expected 1 added value, got %d", len(added))
	}
	if added[0] != "tag3" {
		t.Errorf("expected tag3, got %s", added[0])
	}
	deleted := row.getDeletedPartKeyValues(v10, v11)
	if len(deleted) != 1 {
		t.Fatalf("expected 1 deleted value, got %d", len(deleted))
	}
	if deleted[0] != "tag2" {
		t.Errorf("expected tag2, got %s", deleted[0])
	}
}

func TestTable_ReadPartKeyValues(t *testing.T) {
	// Read all the entity IDs from the table
	ids, err := testTable.ReadAllPartKeyValues(ctx, testTable.GetEntityRow())
	if err != nil {
		t.Fatalf("ReadAllPartKeyValues failed: %v", err)
	} else if len(ids) == 0 {
		t.Fatalf("expected entity ID(s), got %d", len(ids))
	}

	// Count the number of entities in the table
	count, err := testTable.CountPartKeyValues(ctx, testTable.GetEntityRow())
	if err != nil {
		t.Errorf("CountPartKeyValues failed: %v", err)
	} else if count != int64(len(ids)) {
		t.Errorf("expected entity count, got %d", count)
	}

	// Read paginated entity IDs from the table (forward sort order)
	page, err := testTable.ReadPartKeyValues(ctx, testTable.GetEntityRow(), false, 1, tuid.MinID)
	if err != nil {
		t.Errorf("ReadPartKeyValues failed: %v", err)
	} else if len(page) == 0 {
		t.Errorf("expected an entity ID, got %d", len(ids))
	} else if ids[0] != page[0] {
		t.Errorf("expected %s, got %s", ids[0], page[0])
	}

	// Read paginated entity IDs from the table (reverse sort order)
	page, err = testTable.ReadPartKeyValues(ctx, testTable.GetEntityRow(), true, 1, tuid.MaxID)
	if err != nil {
		t.Errorf("ReadPartKeyValues failed: %v", err)
	} else if len(page) == 0 {
		t.Errorf("expected an entity ID, got %d", len(ids))
	} else if ids[len(ids)-1] != page[0] {
		t.Errorf("expected %s, got %s", ids[len(ids)-1], page[0])
	}
}

func TestTable_ReadSortKeyValues(t *testing.T) {
	// Read all sort key values from the table row
	row, ok := testTable.GetRow("things_day")
	if !ok {
		t.Fatalf("things_day row not found")
	}
	if !row.IsValid() {
		t.Fatalf("things_day row is not valid")
	}
	partKeyValue := row.PartKeyValue(v10)
	sortKeyValues, err := testTable.ReadAllSortKeyValues(ctx, row, partKeyValue)
	if err != nil {
		t.Fatalf("ReadAllSortKeyValues failed: %v", err)
	} else if len(sortKeyValues) < 2 {
		t.Fatalf("expected 2 sort key value(s), got %d", len(sortKeyValues))
	}

	// Count the number of sort key values in the table row
	count, err := testTable.CountSortKeyValues(ctx, row, partKeyValue)
	if err != nil {
		t.Errorf("CountSortKeyValues failed: %v", err)
	} else if count != int64(len(sortKeyValues)) {
		t.Errorf("expected sort key value count, got %d", count)
	}

	// Read paginated sort key values from the table row (forward sort order)
	page, err := testTable.ReadSortKeyValues(ctx, row, partKeyValue, false, 1, tuid.MinID)
	if err != nil {
		t.Errorf("ReadSortKeyValues failed: %v", err)
	} else if len(page) == 0 {
		t.Errorf("expected a sort key value, got %d", len(sortKeyValues))
	} else if sortKeyValues[0] != page[0] {
		t.Errorf("expected %s, got %s", sortKeyValues[0], page[0])
	}

	// Read paginated sort key values from the table row (reverse sort order)
	page, err = testTable.ReadSortKeyValues(ctx, row, partKeyValue, true, 1, tuid.MaxID)
	if err != nil {
		t.Errorf("ReadSortKeyValues failed: %v", err)
	} else if len(page) == 0 {
		t.Errorf("expected a sort key value, got %d", len(sortKeyValues))
	} else if sortKeyValues[len(sortKeyValues)-1] != page[0] {
		t.Errorf("expected %s, got %s", sortKeyValues[len(sortKeyValues)-1], page[0])
	}

	// Read the first sort key value from the table row
	sortKeyValue, err := testTable.ReadFirstSortKeyValue(ctx, row, partKeyValue)
	if err != nil {
		t.Errorf("ReadFirstSortKeyValue failed: %v", err)
	} else if sortKeyValue != sortKeyValues[0] {
		t.Errorf("expected %s, got %s", sortKeyValues[0], sortKeyValue)
	}

	// Read the last sort key value from the table row
	sortKeyValue, err = testTable.ReadLastSortKeyValue(ctx, row, partKeyValue)
	if err != nil {
		t.Errorf("ReadLastSortKeyValue failed: %v", err)
	} else if sortKeyValue != sortKeyValues[len(sortKeyValues)-1] {
		t.Errorf("expected %s, got %s", sortKeyValues[len(sortKeyValues)-1], sortKeyValue)
	}
}

func TestTable_EntityExists(t *testing.T) {
	// Check if an entity exists
	exists := testTable.EntityExists(ctx, v10.ID)
	if !exists {
		t.Errorf("expected entity %s to exist", v10.ID)
	}

	// Check if an entity does not exist
	id := tuid.NewID()
	exists = testTable.EntityExists(ctx, id.String())
	if exists {
		t.Errorf("expected entity %s to not exist", id)
	}
}

func TestTable_EntityVersionExists(t *testing.T) {
	// Check if an entity version exists
	exists := testTable.EntityVersionExists(ctx, v10.ID, v10.UpdateID)
	if !exists {
		t.Errorf("expected entity %s version %s to exist", v10.ID, v10.UpdateID)
	}

	// Check if an entity version does not exist
	id := tuid.NewID()
	exists = testTable.EntityVersionExists(ctx, id.String(), v10.UpdateID)
	if exists {
		t.Errorf("expected entity %s version %s to not exist", id, v10.UpdateID)
	}
}

func TestTable_ReadEntities(t *testing.T) {
	ids := []string{v11.ID, v20.ID}
	var entities = testTable.ReadEntities(ctx, ids)
	if len(entities) != 2 {
		t.Fatalf("expected 2 entities, got %d", len(entities))
	}
	for _, e := range entities {
		if v11.ID == e.ID {
			if !reflect.DeepEqual(v11, e) {
				t.Errorf("v11 and e are not equal")
			}
		} else if v20.ID == e.ID {
			if !reflect.DeepEqual(v20, e) {
				t.Errorf("v20 and e are not equal")
			}
		} else {
			t.Errorf("unexpected entity ID: %s", e.ID)
		}
	}
}

func TestTable_ReadEntities_missing(t *testing.T) {
	ids := []string{v11.ID, "not found", v20.ID}
	var entities = testTable.ReadEntities(ctx, ids)
	if len(entities) != 2 {
		t.Fatalf("expected 2 entities, got %d", len(entities))
	}
	for _, e := range entities {
		if v11.ID == e.ID {
			if !reflect.DeepEqual(v11, e) {
				t.Errorf("v11 and e are not equal")
			}
		} else if v20.ID == e.ID {
			if !reflect.DeepEqual(v20, e) {
				t.Errorf("v20 and e are not equal")
			}
		} else {
			t.Errorf("unexpected entity ID: %s", e.ID)
		}
	}
}

func TestTable_ReadEntitiesAsJSON(t *testing.T) {
	ids := []string{v11.ID, v20.ID}
	jsonBytes := testTable.ReadEntitiesAsJSON(ctx, ids)
	if len(jsonBytes) == 0 {
		t.Fatalf("expected JSON bytes, got %d", len(jsonBytes))
	}
	jsonString := string(jsonBytes)
	if !strings.Contains(jsonString, v11.ID) {
		t.Errorf("expected JSON string to contain entity ID %s, got %s", v11.ID, jsonString)
	}
	if !strings.Contains(jsonString, v20.ID) {
		t.Errorf("expected JSON string to contain entity ID %s, got %s", v20.ID, jsonString)
	}
}

func TestTable_ReadEntitiesAsJSON_missing(t *testing.T) {
	ids := []string{v11.ID, "not found", v20.ID}
	jsonBytes := testTable.ReadEntitiesAsJSON(ctx, ids)
	if len(jsonBytes) == 0 {
		t.Fatalf("expected JSON bytes, got %d", len(jsonBytes))
	}
	jsonString := string(jsonBytes)
	if !strings.Contains(jsonString, v11.ID) {
		t.Errorf("expected JSON string to contain entity ID %s, got %s", v11.ID, jsonString)
	}
	if !strings.Contains(jsonString, v20.ID) {
		t.Errorf("expected JSON string to contain entity ID %s, got %s", v20.ID, jsonString)
	}
	if strings.Contains(jsonString, ",,") || strings.Contains(jsonString, "[,") || strings.Contains(jsonString, ",]") {
		t.Errorf("missing id produced malformed json with ',,' or '[,' or ',] %s", jsonString)
	}
}

func TestTable_ReadEntity(t *testing.T) {
	// Read the current version of an entity
	v11Check, err := testTable.ReadEntity(ctx, v11.ID)
	if err != nil {
		t.Errorf("ReadEntity failed: %v", err)
	} else if !reflect.DeepEqual(v11, v11Check) {
		t.Errorf("v11 and v11Check are not equal")
	}
	// Read an entity that does not exist
	_, err = testTable.ReadEntity(ctx, "does-not-exist")
	if err == nil {
		t.Errorf("expected an error, got nil")
	}
	// Read an entity as JSON
	jsonBytes, err := testTable.ReadEntityAsJSON(ctx, v11.ID)
	if err != nil {
		t.Errorf("ReadEntityAsJSON failed: %v", err)
	} else if !strings.Contains(string(jsonBytes), v11.ID) {
		t.Errorf("expected JSON to contain entity ID, got %s", string(jsonBytes))
	}
}

func TestTable_ReadEntityVersion(t *testing.T) {
	v10Check, err := testTable.ReadEntityVersion(ctx, v10.ID, v10.UpdateID)
	if err != nil {
		t.Errorf("ReadEntityVersion failed: %v", err)
	} else if !reflect.DeepEqual(v10, v10Check) {
		t.Errorf("v10 and v10Check are not equal")
	}
}

func TestTable_ReadEntityVersions(t *testing.T) {
	// Read all entity versions
	var versions []versionableThing
	versions, err := testTable.ReadAllEntityVersions(ctx, v10.ID)
	if err != nil {
		t.Fatalf("ReadAllEntityVersions failed: %v", err)
	} else if len(versions) == 0 {
		t.Fatalf("expected multiple entity versions, got %d", len(versions))
	}

	// Read paginated entity versions (forward sort order)
	var page []versionableThing
	page, err = testTable.ReadEntityVersions(ctx, v10.ID, false, 1, tuid.MinID)
	if err != nil {
		t.Errorf("ReadEntityVersions failed: %v", err)
	} else if len(page) == 0 {
		t.Errorf("expected an entity version, got %d", len(page))
	} else if !reflect.DeepEqual(versions[0], page[0]) {
		t.Errorf("paginated entity versions are not equal (forward)")
	}

	// Read paginated entity versions (reverse sort order)
	page, err = testTable.ReadEntityVersions(ctx, v10.ID, true, 1, tuid.MaxID)
	if err != nil {
		t.Errorf("ReadEntityVersions failed: %v", err)
	} else if len(page) == 0 {
		t.Errorf("expected an entity version, got %d", len(page))
	} else if !reflect.DeepEqual(versions[len(versions)-1], page[0]) {
		t.Errorf("paginated entity versions are not equal (reverse)")
	}
}

func TestTable_ReadEntityFromRow(t *testing.T) {
	row, ok := testTable.GetRow("things_day")
	if !ok {
		t.Fatalf("things_day row not found")
	}
	if !row.IsValid() {
		t.Fatalf("things_day row is invalid")
	}
	partKeyValue := row.PartKeyValue(v20)
	sortKeyValue := row.SortKeyValue(v20)
	v20Check, err := testTable.ReadEntityFromRow(ctx, row, partKeyValue, sortKeyValue)
	if err != nil {
		t.Errorf("ReadEntityFromRow failed: %v", err)
	} else if !reflect.DeepEqual(v20, v20Check) {
		t.Errorf("v20 and v20Check are not equal")
	}
}

func TestTable_ReadEntitiesFromRow(t *testing.T) {
	row, ok := testTable.GetRow("things_day")
	if !ok {
		t.Fatalf("things_day row not found")
	}
	if !row.IsValid() {
		t.Fatalf("things_day row is invalid")
	}
	day := row.PartKeyValue(v11)
	var entities []versionableThing
	entities, err := testTable.ReadAllEntitiesFromRow(ctx, row, day)
	if err != nil {
		t.Fatalf("ReadAllEntitiesFromRow failed: %v", err)
	} else if len(entities) < 2 {
		t.Fatalf("expected 2 entities, got %d", len(entities))
	}
	if !reflect.DeepEqual(v11, entities[0]) {
		t.Errorf("v10 and entities[0] are not equal")
	}
	if !reflect.DeepEqual(v20, entities[1]) {
		t.Errorf("v20 and entities[1] are not equal")
	}

	// Read paginated entities (forward sort order)
	entities, err = testTable.ReadEntitiesFromRow(ctx, row, day, false, 1, tuid.MinID)
	if err != nil {
		t.Errorf("ReadEntitiesFromRow failed: %v", err)
	} else if len(entities) == 0 {
		t.Errorf("expected an entity, got %d", len(entities))
	} else if !reflect.DeepEqual(v11, entities[0]) {
		t.Errorf("paginated entities are not equal (forward)")
	}

	// Read paginated entities (reverse sort order)
	entities, err = testTable.ReadEntitiesFromRow(ctx, row, day, true, 1, tuid.MaxID)
	if err != nil {
		t.Errorf("ReadEntitiesFromRow failed: %v", err)
	} else if len(entities) == 0 {
		t.Errorf("expected an entity, got %d", len(entities))
	} else if !reflect.DeepEqual(v20, entities[0]) {
		t.Errorf("paginated entities are not equal (reverse)")
	}
}

func TestTable_ReadEntitiesFromRowAsJSON(t *testing.T) {
	row, ok := testTable.GetRow("things_day")
	if !ok {
		t.Fatalf("things_day row not found")
	}
	if !row.IsValid() {
		t.Fatalf("things_day row is invalid")
	}
	day := row.PartKeyValue(v11)
	jsonBytes, err := testTable.ReadAllEntitiesFromRowAsJSON(ctx, row, day)
	if err != nil {
		t.Fatalf("ReadAllEntitiesFromRowAsJSON failed: %v", err)
	} else if len(jsonBytes) == 0 {
		t.Fatalf("expected JSON, got %d", len(jsonBytes))
	}
	jsonString := string(jsonBytes)
	if !strings.Contains(jsonString, v11.ID) {
		t.Errorf("expected JSON to contain entity ID %s, got %s", v11.ID, jsonString)
	}
	if !strings.Contains(jsonString, v20.ID) {
		t.Errorf("expected JSON to contain entity ID %s, got %s", v20.ID, jsonString)
	}
}

func TestTable_ReadRecord(t *testing.T) {
	row, ok := testTable.GetRow("messages_tag")
	if !ok {
		t.Fatalf("messages_tag row not found")
	}
	if !row.IsValid() {
		t.Fatalf("messages_tag row is invalid")
	}

	// Read an expected Record
	record, err := testTable.ReadRecord(ctx, row, "tag1", v11.ID)
	if err != nil {
		t.Fatalf("ReadRecord failed: %v", err)
	}
	if record.PartKeyValue != "messages_tag|tag|tag1" {
		t.Errorf("expected part key value 'messages_tag|tag|tag1', got %s", record.PartKeyValue)
	}
	if record.SortKeyValue != v11.ID {
		t.Errorf("expected sort key value %s, got %s", v11.ID, record.SortKeyValue)
	}
	if record.JsonValue != nil {
		t.Errorf("expected binary value nil, got %v", record.JsonValue)
	}
	if record.TextValue != v11.Message {
		t.Errorf("expected text value %s, got %s", v11.Message, record.TextValue)
	}
	if record.NumericValue != float64(v11.Count) {
		t.Errorf("expected numeric value %d, got %f", v11.Count, record.NumericValue)
	}
	if record.TimeToLive != v11.ExpiresAt.Unix() {
		t.Errorf("expected expires at %d, got %d", v11.ExpiresAt.Unix(), record.TimeToLive)
	}

	// Read a non-existent Record
	record, err = testTable.ReadRecord(ctx, row, "tag2", v11.ID)
	if err == nil {
		t.Fatalf("expected ReadRecord not found, received: %v", record)
	} else if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected ReadRecord not found, received: %v", err)
	}
}

func TestTable_ReadRecords(t *testing.T) {
	row, ok := testTable.GetRow("messages_tag")
	if !ok {
		t.Fatalf("messages_tag row not found")
	}
	if !row.IsValid() {
		t.Fatalf("messages_tag row is invalid")
	}
	tag := "tag1"

	// Read all Records, sorted by key
	var records []Record
	records, err := testTable.ReadAllRecords(ctx, row, tag)
	if err != nil {
		t.Fatalf("ReadAllRecords failed: %v", err)
	} else if len(records) < 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}

	// Read paginated Records (forward sort order)
	records, err = testTable.ReadRecords(ctx, row, tag, false, 1, tuid.MinID)
	if err != nil {
		t.Fatalf("ReadRecords failed: %v", err)
	} else if len(records) == 0 {
		t.Errorf("expected a Record, got %d", len(records))
	} else if v11.ID != records[0].SortKeyValue {
		t.Errorf("paginated Records are not equal (forward)")
	}

	// Read paginated Records (reverse sort order)
	records, err = testTable.ReadRecords(ctx, row, tag, true, 1, tuid.MaxID)
	if err != nil {
		t.Fatalf("ReadRecords failed: %v", err)
	} else if len(records) == 0 {
		t.Errorf("expected a Record, got %d", len(records))
	} else if v20.ID != records[0].SortKeyValue {
		t.Errorf("paginated Records are not equal (reverse)")
	}
}

func TestTable_ReadEntityLabel(t *testing.T) {
	// Read an expected entity label
	label, err := testTable.ReadEntityLabel(ctx, v11.ID)
	if err != nil {
		t.Errorf("ReadEntityLabel failed: %v", err)
	} else if label.Value != v11.Message {
		t.Errorf("expected label %s, got %s", v11.Message, label.Value)
	}

	// Read a non-existent entity label
	label, err = testTable.ReadEntityLabel(ctx, tuid.NewID().String())
	if err == nil {
		t.Errorf("expected ReadEntityLabel not found, received: %s", label)
	} else if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ReadEntityLabel not found, received: %s", err.Error())
	}

	// Read a partition key label from a row that does not have one
	row, ok := testTable.GetRow("messages_tag")
	if ok {
		label, err = testTable.ReadPartKeyLabel(ctx, row, "tag1")
		if err == nil {
			t.Errorf("expected ReadPartKeyLabel row config error, received none")
		}
	} else {
		t.Errorf("messages_tag row not found")
	}
}

func TestTable_ReadEntityLabels(t *testing.T) {
	// Read all entity labels, sorted by value
	labels, err := testTable.ReadAllEntityLabels(ctx, true)
	if err != nil {
		t.Errorf("ReadAllEntityLabels failed: %v", err)
	} else if len(labels) < 2 {
		t.Errorf("expected 2 labels, got %d", len(labels))
	} else {
		// Check that the labels are sorted
		for i := 0; i < len(labels)-1; i++ {
			if labels[i].Value > labels[i+1].Value {
				t.Errorf("labels are not sorted")
			}
		}
		if labels[0].Value != v20.Message {
			t.Errorf("expected label %s, got %s", v20.Message, labels[0].Value)
		}
		if labels[1].Value != v11.Message {
			t.Errorf("expected label %s, got %s", v11.Message, labels[1].Value)
		}
	}

	// Filter entity labels (contains any term)
	terms := strings.Fields(strings.ToLower(v11.Message + " " + v20.Message))
	labels, err = testTable.FilterEntityLabels(ctx, func(tv TextValue) bool {
		return tv.ContainsAny(terms)
	})
	if err != nil {
		t.Errorf("FilterEntityLabels failed: %v", err)
	} else if len(labels) != 2 {
		t.Errorf("expected 2 entity labels, got %d", len(labels))
	}

	// Filter entity labels (contains all terms)
	labels, err = testTable.FilterEntityLabels(ctx, func(tv TextValue) bool {
		return tv.ContainsAll(terms)
	})
	if err != nil {
		t.Errorf("FilterEntityLabels failed: %v", err)
	} else if len(labels) != 0 {
		t.Errorf("expected 0 entity labels, got %d", len(labels))
	}

	// Read paginated entity labels (forward sort order)
	labels, err = testTable.ReadEntityLabels(ctx, false, 1, "")
	if err != nil {
		t.Errorf("ReadEntityLabels failed: %v", err)
	} else if len(labels) == 0 {
		t.Errorf("expected an entity label, got %d", len(labels))
	} else if !reflect.DeepEqual(TextValue{Key: v11.ID, Value: v11.Message}, labels[0]) {
		t.Errorf("paginated entity labels are not equal (forward)")
	}

	// Read paginated entity labels (reverse sort order)
	labels, err = testTable.ReadEntityLabels(ctx, true, 1, "")
	if err != nil {
		t.Errorf("ReadEntityLabels failed: %v", err)
	} else if len(labels) == 0 {
		t.Errorf("expected a text value, got %d", len(labels))
	} else if !reflect.DeepEqual(TextValue{Key: v20.ID, Value: v20.Message}, labels[0]) {
		t.Errorf("paginated entity labels are not equal (reverse)")
	}
}

func TestTable_ReadTextValue(t *testing.T) {
	row, ok := testTable.GetRow("messages_tag")
	if !ok {
		t.Fatalf("messages_tag row not found")
	}
	if !row.IsValid() {
		t.Fatalf("messages_tag row is invalid")
	}

	// Read an expected text value
	textValue, err := testTable.ReadTextValue(ctx, row, "tag1", v11.ID)
	if err != nil {
		t.Fatalf("ReadTextValue failed: %v", err)
	}
	if textValue.Key != v11.ID {
		t.Errorf("expected TextValue.Key %s, got %s", v11.ID, textValue.Key)
	}
	if textValue.Value != v11.Message {
		t.Errorf("expected TextValue.Value %s, got %s", v11.Message, textValue.Value)
	}

	// Read a non-existent text value
	textValue, err = testTable.ReadTextValue(ctx, row, "tag2", v11.ID)
	if err == nil {
		t.Fatalf("expected ReadTextValue not found, received: %v", textValue)
	} else if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected ReadTextValue not found, received: %v", err)
	}
}

func TestTable_ReadTextValues(t *testing.T) {
	row, ok := testTable.GetRow("messages_tag")
	if !ok {
		t.Fatalf("messages_tag row not found")
	}
	if !row.IsValid() {
		t.Fatalf("messages_tag row is invalid")
	}
	tag := "tag1"

	// Read all text values, sorted by key
	var textValues []TextValue
	textValues, err := testTable.ReadAllTextValues(ctx, row, tag, false)
	if err != nil {
		t.Fatalf("ReadAllTextValues failed: %v", err)
	} else if len(textValues) < 2 {
		t.Fatalf("expected 2 text values, got %d", len(textValues))
	}
	if !reflect.DeepEqual(TextValue{Key: v11.ID, Value: v11.Message}, textValues[0]) {
		t.Errorf("v11 and textValues[0] are not equal")
	}
	if !reflect.DeepEqual(TextValue{Key: v20.ID, Value: v20.Message}, textValues[1]) {
		t.Errorf("v20 and textValues[1] are not equal")
	}

	// Read all text values, sorted by value
	textValues, err = testTable.ReadAllTextValues(ctx, row, tag, true)
	if err != nil {
		t.Fatalf("ReadAllTextValues failed: %v", err)
	} else if len(textValues) < 2 {
		t.Fatalf("expected 2 text values, got %d", len(textValues))
	}
	if !reflect.DeepEqual(TextValue{Key: v20.ID, Value: v20.Message}, textValues[0]) {
		t.Errorf("v20 and textValues[0] are not equal")
	}
	if !reflect.DeepEqual(TextValue{Key: v11.ID, Value: v11.Message}, textValues[1]) {
		t.Errorf("v11 and textValues[1] are not equal")
	}

	// Filter text values (contains any term)
	terms := []string{"zyx", "v2"}
	textValues, err = testTable.FilterTextValues(ctx, row, tag, func(tv TextValue) bool {
		return tv.ContainsAny(terms)
	})
	if err != nil {
		t.Errorf("FilterTextValues failed: %v", err)
	} else if len(textValues) != 2 {
		t.Errorf("expected 2 text values, got %d", len(textValues))
	}

	// Filter text values (contains all terms)
	textValues, err = testTable.FilterTextValues(ctx, row, tag, func(tv TextValue) bool {
		return tv.ContainsAll(terms)
	})
	if err != nil {
		t.Errorf("FilterTextValues failed: %v", err)
	} else if len(textValues) != 0 {
		t.Errorf("expected 0 text values, got %d", len(textValues))
	}

	// Read paginated text values (forward sort order)
	textValues, err = testTable.ReadTextValues(ctx, row, tag, false, 1, tuid.MinID)
	if err != nil {
		t.Errorf("ReadTextValues failed: %v", err)
	} else if len(textValues) == 0 {
		t.Errorf("expected a text value, got %d", len(textValues))
	} else if !reflect.DeepEqual(TextValue{Key: v11.ID, Value: v11.Message}, textValues[0]) {
		t.Errorf("paginated text values are not equal (forward)")
	}

	// Read paginated text values (reverse sort order)
	textValues, err = testTable.ReadTextValues(ctx, row, tag, true, 1, tuid.MaxID)
	if err != nil {
		t.Errorf("ReadTextValues failed: %v", err)
	} else if len(textValues) == 0 {
		t.Errorf("expected a text value, got %d", len(textValues))
	}
}

func TestTable_ReadNumericValue(t *testing.T) {
	row, ok := testTable.GetRow("messages_tag")
	if !ok {
		t.Fatalf("messages_tag row not found")
	}
	if !row.IsValid() {
		t.Fatalf("messages_tag row is invalid")
	}

	// Read an expected numeric value
	numValue, err := testTable.ReadNumericValue(ctx, row, "tag1", v11.ID)
	if err != nil {
		t.Fatalf("ReadNumericValue failed: %v", err)
	}
	if numValue.Key != v11.ID {
		t.Errorf("expected NumValue.Key %s, got %s", v11.ID, numValue.Key)
	}
	if numValue.Value != float64(v11.Count) {
		t.Errorf("expected NumValue.Value %v, got %v", v11.Count, numValue.Value)
	}

	// Read a non-existent numeric value
	numValue, err = testTable.ReadNumericValue(ctx, row, "tag2", v11.ID)
	if err == nil {
		t.Fatalf("expected ReadNumericValue not found, received: %v", numValue)
	} else if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected ReadNumericValue not found, received: %v", err)
	}
}

func TestTable_ReadNumericValues(t *testing.T) {
	row, ok := testTable.GetRow("messages_tag")
	if !ok {
		t.Fatalf("messages_tag row not found")
	}
	if !row.IsValid() {
		t.Fatalf("messages_tag row is invalid")
	}
	tag := "tag1"

	// Read all numeric values, sorted by key
	var numValues []NumValue
	numValues, err := testTable.ReadAllNumericValues(ctx, row, tag, false)
	if err != nil {
		t.Fatalf("ReadAllNumericValues failed: %v", err)
	} else if len(numValues) < 2 {
		t.Fatalf("expected 2 numeric values, got %d", len(numValues))
	}
	if !reflect.DeepEqual(NumValue{Key: v11.ID, Value: float64(v11.Count)}, numValues[0]) {
		t.Errorf("v11 and numValues[0] are not equal")
	}
	if !reflect.DeepEqual(NumValue{Key: v20.ID, Value: float64(v20.Count)}, numValues[1]) {
		t.Errorf("v20 and numValues[1] are not equal")
	}

	// Read all numeric values, sorted by value
	numValues, err = testTable.ReadAllNumericValues(ctx, row, tag, true)
	if err != nil {
		t.Fatalf("ReadAllNumericValues failed: %v", err)
	} else if len(numValues) < 2 {
		t.Fatalf("expected 2 numeric values, got %d", len(numValues))
	}
	if !reflect.DeepEqual(NumValue{Key: v20.ID, Value: float64(v20.Count)}, numValues[0]) {
		t.Errorf("v20 and numValues[0] are not equal")
	}
	if !reflect.DeepEqual(NumValue{Key: v11.ID, Value: float64(v11.Count)}, numValues[1]) {
		t.Errorf("v11 and numValues[1] are not equal")
	}

	// Read paginated numeric values (forward sort order)
	numValues, err = testTable.ReadNumericValues(ctx, row, tag, false, 1, tuid.MinID)
	if err != nil {
		t.Errorf("ReadNumericValues failed: %v", err)
	} else if len(numValues) == 0 {
		t.Errorf("expected a numeric value, got %d", len(numValues))
	} else if !reflect.DeepEqual(NumValue{Key: v11.ID, Value: float64(v11.Count)}, numValues[0]) {
		t.Errorf("paginated numeric values are not equal (forward)")
	}

	// Read paginated numeric values (reverse sort order)
	numValues, err = testTable.ReadNumericValues(ctx, row, tag, true, 1, tuid.MaxID)
	if err != nil {
		t.Errorf("ReadNumericValues failed: %v", err)
	} else if len(numValues) == 0 {
		t.Errorf("expected a numeric value, got %d", len(numValues))
	} else if !reflect.DeepEqual(NumValue{Key: v20.ID, Value: float64(v20.Count)}, numValues[0]) {
		t.Errorf("paginated numeric values are not equal (reverse)")
	}
}

// Initialize the test database with sample data
func initTestDatabase(ctx context.Context, writer TableWriter[versionableThing]) {
	// Write initial versions of the test data
	err := writer.WriteEntity(ctx, v10)
	if err != nil {
		fmt.Printf("v1.0 write failed: %v\n", err)
		os.Exit(1)
	}
	err = writer.WriteEntity(ctx, v20)
	if err != nil {
		fmt.Printf("v2.0 write failed: %v\n", err)
		os.Exit(1)
	}

	// Update the first entity (which should remove tag2 and add tag3)
	err = writer.UpdateEntity(ctx, v11)
	if err != nil {
		fmt.Printf("v1.1 write failed: %v\n", err)
		os.Exit(1)
	}
}

// versionableThing is a simple entity that can be versioned (it has an ID and an UpdateID).
// It is used for testing entity Table functionality.
type versionableThing struct {
	ID        string    `json:"id"`
	UpdateID  string    `json:"updateId"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
	ExpiresAt time.Time `json:"expiresAt"`
	Tags      []string  `json:"tags"`
	Count     int       `json:"count"`
	Message   string    `json:"message"`
}

// CreatedOn returns an ISO-8601 formatted string date from the CreatedAt timestamp.
// It is used for testing entity Table functionality (grouping things by date).
func (v versionableThing) CreatedOn() string {
	if v.CreatedAt.IsZero() {
		return ""
	}
	return v.CreatedAt.Format("2006-01-02")
}

// UpdatedOn returns an ISO-8601 formatted string date from the UpdatedAt timestamp.
// It is used for testing entity Table functionality (grouping things by date).
func (v versionableThing) UpdatedOn() string {
	if v.UpdatedAt.IsZero() {
		return ""
	}
	return v.UpdatedAt.Format("2006-01-02")
}

// CompressedJSON returns a compressed JSON representation of the versionableThing.
func (v versionableThing) CompressedJSON() []byte {
	gzJSON, _ := ToCompressedJSON(v)
	return gzJSON
}

// newVersionableThing creates a new versionableThing with a new ID and UpdateID based on the current system time.
func newVersionableThing(msg string, count int, tags []string) versionableThing {
	id := tuid.NewID()
	createdAt, _ := id.Time()
	return versionableThing{
		ID:        id.String(),
		UpdateID:  id.String(),
		CreatedAt: createdAt,
		UpdatedAt: createdAt,
		ExpiresAt: createdAt.AddDate(1, 0, 0), // expires in one year
		Tags:      tags,
		Count:     count,
		Message:   msg,
	}
}

// updatedVersionableThing updates the message of a versionableThing, along with it's UpdateID and UpdatedAt timestamp.
func updatedVersionableThing(v versionableThing, msg string, count int, tags []string) versionableThing {
	updateID := tuid.NewID()
	updatedAt, _ := updateID.Time()
	return versionableThing{
		ID:        v.ID,
		UpdateID:  updateID.String(),
		CreatedAt: v.CreatedAt,
		UpdatedAt: updatedAt,
		ExpiresAt: updatedAt.AddDate(1, 0, 0), // expires in one year
		Tags:      tags,
		Count:     count,
		Message:   msg,
	}
}

// newThingTable constructs a new Table definition for VersionableThings, complete with TableRow definitions.
func newThingTable(dbClient *dynamodb.Client, env string) Table[versionableThing] {

	// TableRow: entity versions, partitioned by ID and ordered by UpdateID
	thingsVersion := TableRow[versionableThing]{
		RowName:       "things_version",
		PartKeyName:   "id",
		PartKeyValue:  func(v versionableThing) string { return v.ID },
		PartKeyValues: nil,
		PartKeyLabel:  func(v versionableThing) string { return v.Message },
		SortKeyName:   "update_id",
		SortKeyValue:  func(v versionableThing) string { return v.UpdateID },
		JsonValue:     func(v versionableThing) []byte { return v.CompressedJSON() },
		TextValue:     nil,
		NumericValue:  nil,
		TimeToLive:    func(v versionableThing) int64 { return v.ExpiresAt.Unix() },
	}

	// TableRow: things by day, partitioned by CreatedOn date and ordered by ID
	thingsDay := TableRow[versionableThing]{
		RowName:       "things_day",
		PartKeyName:   "day",
		PartKeyValue:  func(v versionableThing) string { return v.CreatedOn() },
		PartKeyValues: nil,
		SortKeyName:   "id",
		SortKeyValue:  func(v versionableThing) string { return v.ID },
		JsonValue:     func(v versionableThing) []byte { return v.CompressedJSON() },
		TextValue:     nil,
		NumericValue:  nil,
		TimeToLive:    func(v versionableThing) int64 { return v.ExpiresAt.Unix() },
	}

	// TableRow: messages by topic, partitioned by Topic and ordered by ID
	messagesTag := TableRow[versionableThing]{
		RowName:       "messages_tag",
		PartKeyName:   "tag",
		PartKeyValue:  nil,
		PartKeyValues: func(v versionableThing) []string { return v.Tags },
		SortKeyName:   "id",
		SortKeyValue:  func(v versionableThing) string { return v.ID },
		JsonValue:     nil,
		TextValue:     func(v versionableThing) string { return v.Message },
		NumericValue:  func(v versionableThing) float64 { return float64(v.Count) },
		TimeToLive:    func(v versionableThing) int64 { return v.ExpiresAt.Unix() },
	}

	return Table[versionableThing]{
		Client:           dbClient,
		EntityType:       "versionableThing",
		TableName:        "versionable_things_" + env,
		PartKeyAttr:      "part_key",
		SortKeyAttr:      "sort_key",
		JsonValueAttr:    "json_value",
		TextValueAttr:    "text_value",
		NumericValueAttr: "num_value",
		TimeToLiveAttr:   "expires_at",
		TTL:              true,
		EntityRow:        thingsVersion,
		IndexRows: map[string]TableRow[versionableThing]{
			thingsDay.RowName:   thingsDay,
			messagesTag.RowName: messagesTag,
		},
	}
}

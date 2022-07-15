package versionary

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/voxtechnica/tuid-go"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

var (
	ctx         context.Context
	tableReader TableReader[VersionableThing]
	v10         VersionableThing
	v11         VersionableThing
	v20         VersionableThing
)

func TestMain(m *testing.M) {
	// Set up DynamoDB Client
	ctx = context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		fmt.Printf("unable to load SDK config, %v\n", err)
		os.Exit(1)
	}
	dbClient := dynamodb.NewFromConfig(cfg)

	// Set up Entity Table, creating it if needed
	table := NewThingTable(dbClient, "test")
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

	// Set up Versionable Things
	v10 = CreateVersionableThing("zyx v1.0", 3, []string{"tag1", "tag2"})
	v11 = UpdateVersionableThing(v10, "zyx v1.1", 2, []string{"tag1", "tag3"})
	v20 = CreateVersionableThing("abc v2.0", 1, []string{"tag1", "tag2"})

	// Run all the TableReader tests
	fmt.Println("Running the in-memory TableReader tests ...")
	InitTestDatabase(ctx, memTable)
	tableReader = memTable
	exitVal := m.Run()
	if exitVal != 0 {
		os.Exit(exitVal)
	}

	fmt.Println("Running the DynamoDB TableReader tests ...")
	InitTestDatabase(ctx, table)
	tableReader = table
	exitVal = m.Run()

	// Delete an entity
	err = table.DeleteEntity(ctx, v20)
	if err != nil {
		fmt.Printf("v2.0 DynamoDB delete failed: %v\n", err)
	}
	err = memTable.DeleteEntity(ctx, v20)
	if err != nil {
		fmt.Printf("v2.0 MemTable delete failed: %v\n", err)
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

func TestJSON(t *testing.T) {
	j, err := ToJSON(v10)
	if err != nil {
		t.Fatalf("ToJSON failed: %v", err)
	}
	v10check, err := FromJSON[VersionableThing](j)
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
	v11Check, err := FromCompressedJSON[VersionableThing](gzJSON)
	if err != nil {
		t.Errorf("FromCompressedJSON failed: %v", err)
	} else if !reflect.DeepEqual(v11, v11Check) {
		t.Errorf("v11 and v11Check are not equal")
	}
}

func TestTableRow_GetPartKeyValues(t *testing.T) {
	row, ok := tableReader.GetRow("messages_tag")
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
	ids, err := tableReader.ReadAllPartKeyValues(ctx, tableReader.GetEntityRow())
	if err != nil {
		t.Fatalf("ReadAllPartKeyValues failed: %v", err)
	} else if len(ids) == 0 {
		t.Fatalf("expected entity ID(s), got %d", len(ids))
	}

	// Read paginated entity IDs from the table (forward sort order)
	page, err := tableReader.ReadPartKeyValues(ctx, tableReader.GetEntityRow(), false, 1, tuid.MinID)
	if err != nil {
		t.Errorf("ReadPartKeyValues failed: %v", err)
	} else if len(page) == 0 {
		t.Errorf("expected an entity ID, got %d", len(ids))
	} else if ids[0] != page[0] {
		t.Errorf("expected %s, got %s", ids[0], page[0])
	}

	// Read paginated entity IDs from the table (reverse sort order)
	page, err = tableReader.ReadPartKeyValues(ctx, tableReader.GetEntityRow(), true, 1, tuid.MaxID)
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
	row, ok := tableReader.GetRow("things_day")
	if !ok {
		t.Fatalf("things_day row not found")
	}
	if !row.IsValid() {
		t.Fatalf("things_day row is not valid")
	}
	partKeyValue := row.PartKeyValue(v10)
	sortKeyValues, err := tableReader.ReadAllSortKeyValues(ctx, row, partKeyValue)
	if err != nil {
		t.Fatalf("ReadAllSortKeyValues failed: %v", err)
	} else if len(sortKeyValues) < 2 {
		t.Fatalf("expected 2 sort key value(s), got %d", len(sortKeyValues))
	}

	// Read paginated sort key values from the table row (forward sort order)
	page, err := tableReader.ReadSortKeyValues(ctx, row, partKeyValue, false, 1, tuid.MinID)
	if err != nil {
		t.Errorf("ReadSortKeyValues failed: %v", err)
	} else if len(page) == 0 {
		t.Errorf("expected a sort key value, got %d", len(sortKeyValues))
	} else if sortKeyValues[0] != page[0] {
		t.Errorf("expected %s, got %s", sortKeyValues[0], page[0])
	}

	// Read paginated sort key values from the table row (reverse sort order)
	page, err = tableReader.ReadSortKeyValues(ctx, row, partKeyValue, true, 1, tuid.MaxID)
	if err != nil {
		t.Errorf("ReadSortKeyValues failed: %v", err)
	} else if len(page) == 0 {
		t.Errorf("expected a sort key value, got %d", len(sortKeyValues))
	} else if sortKeyValues[len(sortKeyValues)-1] != page[0] {
		t.Errorf("expected %s, got %s", sortKeyValues[len(sortKeyValues)-1], page[0])
	}

	// Read the first sort key value from the table row
	sortKeyValue, err := tableReader.ReadFirstSortKeyValue(ctx, row, partKeyValue)
	if err != nil {
		t.Errorf("ReadFirstSortKeyValue failed: %v", err)
	} else if sortKeyValue != sortKeyValues[0] {
		t.Errorf("expected %s, got %s", sortKeyValues[0], sortKeyValue)
	}

	// Read the last sort key value from the table row
	sortKeyValue, err = tableReader.ReadLastSortKeyValue(ctx, row, partKeyValue)
	if err != nil {
		t.Errorf("ReadLastSortKeyValue failed: %v", err)
	} else if sortKeyValue != sortKeyValues[len(sortKeyValues)-1] {
		t.Errorf("expected %s, got %s", sortKeyValues[len(sortKeyValues)-1], sortKeyValue)
	}
}

func TestTable_EntityExists(t *testing.T) {
	// Check if an entity exists
	exists := tableReader.EntityExists(ctx, v10.ID)
	if !exists {
		t.Errorf("expected entity %s to exist", v10.ID)
	}

	// Check if an entity does not exist
	id := tuid.NewID()
	exists = tableReader.EntityExists(ctx, id.String())
	if exists {
		t.Errorf("expected entity %s to not exist", id)
	}
}

func TestTable_EntityVersionExists(t *testing.T) {
	// Check if an entity version exists
	exists := tableReader.EntityVersionExists(ctx, v10.ID, v10.UpdateID)
	if !exists {
		t.Errorf("expected entity %s version %s to exist", v10.ID, v10.UpdateID)
	}

	// Check if an entity version does not exist
	id := tuid.NewID()
	exists = tableReader.EntityVersionExists(ctx, id.String(), v10.UpdateID)
	if exists {
		t.Errorf("expected entity %s version %s to not exist", id, v10.UpdateID)
	}
}

func TestTable_ReadEntities(t *testing.T) {
	ids := []string{v11.ID, v20.ID}
	var entities = tableReader.ReadEntities(ctx, ids)
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
	var entities = tableReader.ReadEntities(ctx, ids)
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
	jsonBytes := tableReader.ReadEntitiesAsJSON(ctx, ids)
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
	jsonBytes := tableReader.ReadEntitiesAsJSON(ctx, ids)
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
	v11Check, err := tableReader.ReadEntity(ctx, v11.ID)
	if err != nil {
		t.Errorf("ReadEntity failed: %v", err)
	} else if !reflect.DeepEqual(v11, v11Check) {
		t.Errorf("v11 and v11Check are not equal")
	}
	// Read an entity that does not exist
	_, err = tableReader.ReadEntity(ctx, "does-not-exist")
	if err == nil {
		t.Errorf("expected an error, got nil")
	}
	// Read an entity as JSON
	jsonBytes, err := tableReader.ReadEntityAsJSON(ctx, v11.ID)
	if err != nil {
		t.Errorf("ReadEntityAsJSON failed: %v", err)
	} else if !strings.Contains(string(jsonBytes), v11.ID) {
		t.Errorf("expected JSON to contain entity ID, got %s", string(jsonBytes))
	}
}

func TestTable_ReadEntityVersion(t *testing.T) {
	v10Check, err := tableReader.ReadEntityVersion(ctx, v10.ID, v10.UpdateID)
	if err != nil {
		t.Errorf("ReadEntityVersion failed: %v", err)
	} else if !reflect.DeepEqual(v10, v10Check) {
		t.Errorf("v10 and v10Check are not equal")
	}
}

func TestTable_ReadEntityVersions(t *testing.T) {
	// Read all entity versions
	var versions []VersionableThing
	versions, err := tableReader.ReadAllEntityVersions(ctx, v10.ID)
	if err != nil {
		t.Fatalf("ReadAllEntityVersions failed: %v", err)
	} else if len(versions) == 0 {
		t.Fatalf("expected multiple entity versions, got %d", len(versions))
	}

	// Read paginated entity versions (forward sort order)
	var page []VersionableThing
	page, err = tableReader.ReadEntityVersions(ctx, v10.ID, false, 1, tuid.MinID)
	if err != nil {
		t.Errorf("ReadEntityVersions failed: %v", err)
	} else if len(page) == 0 {
		t.Errorf("expected an entity version, got %d", len(page))
	} else if !reflect.DeepEqual(versions[0], page[0]) {
		t.Errorf("paginated entity versions are not equal (forward)")
	}

	// Read paginated entity versions (reverse sort order)
	page, err = tableReader.ReadEntityVersions(ctx, v10.ID, true, 1, tuid.MaxID)
	if err != nil {
		t.Errorf("ReadEntityVersions failed: %v", err)
	} else if len(page) == 0 {
		t.Errorf("expected an entity version, got %d", len(page))
	} else if !reflect.DeepEqual(versions[len(versions)-1], page[0]) {
		t.Errorf("paginated entity versions are not equal (reverse)")
	}
}

func TestTable_ReadEntityFromRow(t *testing.T) {
	row, ok := tableReader.GetRow("things_day")
	if !ok {
		t.Fatalf("things_day row not found")
	}
	if !row.IsValid() {
		t.Fatalf("things_day row is invalid")
	}
	partKeyValue := row.PartKeyValue(v20)
	sortKeyValue := row.SortKeyValue(v20)
	v20Check, err := tableReader.ReadEntityFromRow(ctx, row, partKeyValue, sortKeyValue)
	if err != nil {
		t.Errorf("ReadEntityFromRow failed: %v", err)
	} else if !reflect.DeepEqual(v20, v20Check) {
		t.Errorf("v20 and v20Check are not equal")
	}
}

func TestTable_ReadEntitiesFromRow(t *testing.T) {
	row, ok := tableReader.GetRow("things_day")
	if !ok {
		t.Fatalf("things_day row not found")
	}
	if !row.IsValid() {
		t.Fatalf("things_day row is invalid")
	}
	day := row.PartKeyValue(v11)
	var entities []VersionableThing
	entities, err := tableReader.ReadAllEntitiesFromRow(ctx, row, day)
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
	entities, err = tableReader.ReadEntitiesFromRow(ctx, row, day, false, 1, tuid.MinID)
	if err != nil {
		t.Errorf("ReadEntitiesFromRow failed: %v", err)
	} else if len(entities) == 0 {
		t.Errorf("expected an entity, got %d", len(entities))
	} else if !reflect.DeepEqual(v11, entities[0]) {
		t.Errorf("paginated entities are not equal (forward)")
	}

	// Read paginated entities (reverse sort order)
	entities, err = tableReader.ReadEntitiesFromRow(ctx, row, day, true, 1, tuid.MaxID)
	if err != nil {
		t.Errorf("ReadEntitiesFromRow failed: %v", err)
	} else if len(entities) == 0 {
		t.Errorf("expected an entity, got %d", len(entities))
	} else if !reflect.DeepEqual(v20, entities[0]) {
		t.Errorf("paginated entities are not equal (reverse)")
	}
}

func TestTable_ReadEntitiesFromRowAsJSON(t *testing.T) {
	row, ok := tableReader.GetRow("things_day")
	if !ok {
		t.Fatalf("things_day row not found")
	}
	if !row.IsValid() {
		t.Fatalf("things_day row is invalid")
	}
	day := row.PartKeyValue(v11)
	jsonBytes, err := tableReader.ReadAllEntitiesFromRowAsJSON(ctx, row, day)
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
	row, ok := tableReader.GetRow("messages_tag")
	if !ok {
		t.Fatalf("messages_tag row not found")
	}
	if !row.IsValid() {
		t.Fatalf("messages_tag row is invalid")
	}

	// Read an expected Record
	record, err := tableReader.ReadRecord(ctx, row, "tag1", v11.ID)
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
	record, err = tableReader.ReadRecord(ctx, row, "tag2", v11.ID)
	if err == nil {
		t.Fatalf("expected ReadRecord not found, received: %v", record)
	} else if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected ReadRecord not found, received: %v", err)
	}
}

func TestTable_ReadRecords(t *testing.T) {
	row, ok := tableReader.GetRow("messages_tag")
	if !ok {
		t.Fatalf("messages_tag row not found")
	}
	if !row.IsValid() {
		t.Fatalf("messages_tag row is invalid")
	}
	tag := "tag1"

	// Read all Records, sorted by key
	var records []Record
	records, err := tableReader.ReadAllRecords(ctx, row, tag)
	if err != nil {
		t.Fatalf("ReadAllRecords failed: %v", err)
	} else if len(records) < 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}

	// Read paginated Records (forward sort order)
	records, err = tableReader.ReadRecords(ctx, row, tag, false, 1, tuid.MinID)
	if err != nil {
		t.Fatalf("ReadRecords failed: %v", err)
	} else if len(records) == 0 {
		t.Errorf("expected a Record, got %d", len(records))
	} else if v11.ID != records[0].SortKeyValue {
		t.Errorf("paginated Records are not equal (forward)")
	}

	// Read paginated Records (reverse sort order)
	records, err = tableReader.ReadRecords(ctx, row, tag, true, 1, tuid.MaxID)
	if err != nil {
		t.Fatalf("ReadRecords failed: %v", err)
	} else if len(records) == 0 {
		t.Errorf("expected a Record, got %d", len(records))
	} else if v20.ID != records[0].SortKeyValue {
		t.Errorf("paginated Records are not equal (reverse)")
	}
}

func TestTable_ReadTextValue(t *testing.T) {
	row, ok := tableReader.GetRow("messages_tag")
	if !ok {
		t.Fatalf("messages_tag row not found")
	}
	if !row.IsValid() {
		t.Fatalf("messages_tag row is invalid")
	}

	// Read an expected text value
	textValue, err := tableReader.ReadTextValue(ctx, row, "tag1", v11.ID)
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
	textValue, err = tableReader.ReadTextValue(ctx, row, "tag2", v11.ID)
	if err == nil {
		t.Fatalf("expected ReadTextValue not found, received: %v", textValue)
	} else if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected ReadTextValue not found, received: %v", err)
	}
}

func TestTable_ReadTextValues(t *testing.T) {
	row, ok := tableReader.GetRow("messages_tag")
	if !ok {
		t.Fatalf("messages_tag row not found")
	}
	if !row.IsValid() {
		t.Fatalf("messages_tag row is invalid")
	}
	tag := "tag1"

	// Read all text values, sorted by key
	var textValues []TextValue
	textValues, err := tableReader.ReadAllTextValues(ctx, row, tag, false)
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
	textValues, err = tableReader.ReadAllTextValues(ctx, row, tag, true)
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

	// Read paginated text values (forward sort order)
	textValues, err = tableReader.ReadTextValues(ctx, row, tag, false, 1, tuid.MinID)
	if err != nil {
		t.Errorf("ReadTextValues failed: %v", err)
	} else if len(textValues) == 0 {
		t.Errorf("expected a text value, got %d", len(textValues))
	} else if !reflect.DeepEqual(TextValue{Key: v11.ID, Value: v11.Message}, textValues[0]) {
		t.Errorf("paginated text values are not equal (forward)")
	}

	// Read paginated text values (reverse sort order)
	textValues, err = tableReader.ReadTextValues(ctx, row, tag, true, 1, tuid.MaxID)
	if err != nil {
		t.Errorf("ReadTextValues failed: %v", err)
	} else if len(textValues) == 0 {
		t.Errorf("expected a text value, got %d", len(textValues))
	}
}

func TestTable_ReadNumericValue(t *testing.T) {
	row, ok := tableReader.GetRow("messages_tag")
	if !ok {
		t.Fatalf("messages_tag row not found")
	}
	if !row.IsValid() {
		t.Fatalf("messages_tag row is invalid")
	}

	// Read an expected numeric value
	numValue, err := tableReader.ReadNumericValue(ctx, row, "tag1", v11.ID)
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
	numValue, err = tableReader.ReadNumericValue(ctx, row, "tag2", v11.ID)
	if err == nil {
		t.Fatalf("expected ReadNumericValue not found, received: %v", numValue)
	} else if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected ReadNumericValue not found, received: %v", err)
	}
}

func TestTable_ReadNumericValues(t *testing.T) {
	row, ok := tableReader.GetRow("messages_tag")
	if !ok {
		t.Fatalf("messages_tag row not found")
	}
	if !row.IsValid() {
		t.Fatalf("messages_tag row is invalid")
	}
	tag := "tag1"

	// Read all numeric values, sorted by key
	var numValues []NumValue
	numValues, err := tableReader.ReadAllNumericValues(ctx, row, tag, false)
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
	numValues, err = tableReader.ReadAllNumericValues(ctx, row, tag, true)
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
	numValues, err = tableReader.ReadNumericValues(ctx, row, tag, false, 1, tuid.MinID)
	if err != nil {
		t.Errorf("ReadNumericValues failed: %v", err)
	} else if len(numValues) == 0 {
		t.Errorf("expected a numeric value, got %d", len(numValues))
	} else if !reflect.DeepEqual(NumValue{Key: v11.ID, Value: float64(v11.Count)}, numValues[0]) {
		t.Errorf("paginated numeric values are not equal (forward)")
	}

	// Read paginated numeric values (reverse sort order)
	numValues, err = tableReader.ReadNumericValues(ctx, row, tag, true, 1, tuid.MaxID)
	if err != nil {
		t.Errorf("ReadNumericValues failed: %v", err)
	} else if len(numValues) == 0 {
		t.Errorf("expected a numeric value, got %d", len(numValues))
	} else if !reflect.DeepEqual(NumValue{Key: v20.ID, Value: float64(v20.Count)}, numValues[0]) {
		t.Errorf("paginated numeric values are not equal (reverse)")
	}
}

// Initialize the test database with sample data
func InitTestDatabase(ctx context.Context, writer TableWriter[VersionableThing]) {
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

// VersionableThing is a simple entity that can be versioned (it has an ID and an UpdateID).
// It is used for testing entity Table functionality.
type VersionableThing struct {
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
func (v VersionableThing) CreatedOn() string {
	if v.CreatedAt.IsZero() {
		return ""
	}
	return v.CreatedAt.Format("2006-01-02")
}

// UpdatedOn returns an ISO-8601 formatted string date from the UpdatedAt timestamp.
// It is used for testing entity Table functionality (grouping things by date).
func (v VersionableThing) UpdatedOn() string {
	if v.UpdatedAt.IsZero() {
		return ""
	}
	return v.UpdatedAt.Format("2006-01-02")
}

// CompressedJSON returns a compressed JSON representation of the VersionableThing.
func (v VersionableThing) CompressedJSON() []byte {
	gzJSON, _ := ToCompressedJSON(v)
	return gzJSON
}

// CreateVersionableThing creates a new VersionableThing with a new ID and UpdateID based on the current system time.
func CreateVersionableThing(msg string, count int, tags []string) VersionableThing {
	id := tuid.NewID()
	createdAt, _ := id.Time()
	return VersionableThing{
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

// UpdateVersionableThing updates the message of a VersionableThing, along with it's UpdateID and UpdatedAt timestamp.
func UpdateVersionableThing(v VersionableThing, msg string, count int, tags []string) VersionableThing {
	updateID := tuid.NewID()
	updatedAt, _ := updateID.Time()
	return VersionableThing{
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

// NewThingTable constructs a new Table definition for VersionableThings, complete with TableRow definitions.
func NewThingTable(dbClient *dynamodb.Client, env string) Table[VersionableThing] {

	// TableRow: entity versions, partitioned by ID and ordered by UpdateID
	thingsVersion := TableRow[VersionableThing]{
		RowName:       "things_version",
		PartKeyName:   "id",
		PartKeyValue:  func(v VersionableThing) string { return v.ID },
		PartKeyValues: nil,
		SortKeyName:   "update_id",
		SortKeyValue:  func(v VersionableThing) string { return v.UpdateID },
		JsonValue:     func(v VersionableThing) []byte { return v.CompressedJSON() },
		TextValue:     nil,
		NumericValue:  nil,
		TimeToLive:    func(v VersionableThing) int64 { return v.ExpiresAt.Unix() },
	}

	// TableRow: things by day, partitioned by CreatedOn date and ordered by ID
	thingsDay := TableRow[VersionableThing]{
		RowName:       "things_day",
		PartKeyName:   "day",
		PartKeyValue:  func(v VersionableThing) string { return v.CreatedOn() },
		PartKeyValues: nil,
		SortKeyName:   "id",
		SortKeyValue:  func(v VersionableThing) string { return v.ID },
		JsonValue:     func(v VersionableThing) []byte { return v.CompressedJSON() },
		TextValue:     nil,
		NumericValue:  nil,
		TimeToLive:    func(v VersionableThing) int64 { return v.ExpiresAt.Unix() },
	}

	// TableRow: messages by topic, partitioned by Topic and ordered by ID
	messagesTag := TableRow[VersionableThing]{
		RowName:       "messages_tag",
		PartKeyName:   "tag",
		PartKeyValue:  nil,
		PartKeyValues: func(v VersionableThing) []string { return v.Tags },
		SortKeyName:   "id",
		SortKeyValue:  func(v VersionableThing) string { return v.ID },
		JsonValue:     nil,
		TextValue:     func(v VersionableThing) string { return v.Message },
		NumericValue:  func(v VersionableThing) float64 { return float64(v.Count) },
		TimeToLive:    func(v VersionableThing) int64 { return v.ExpiresAt.Unix() },
	}

	return Table[VersionableThing]{
		Client:           dbClient,
		EntityType:       "VersionableThing",
		TableName:        "versionable_things_" + env,
		PartKeyAttr:      "part_key",
		SortKeyAttr:      "sort_key",
		JsonValueAttr:    "json_value",
		TextValueAttr:    "text_value",
		NumericValueAttr: "num_value",
		TimeToLiveAttr:   "expires_at",
		TTL:              true,
		EntityRow:        thingsVersion,
		IndexRows: map[string]TableRow[VersionableThing]{
			thingsDay.RowName:   thingsDay,
			messagesTag.RowName: messagesTag,
		},
	}
}

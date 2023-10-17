package thing

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/voxtechnica/tuid-go"
	v "github.com/voxtechnica/versionary"
)

var (
	ctx                = context.Background()
	service            Service
	v10, v11, v20, v30 Thing
)

func TestMain(m *testing.M) {
	// Set up DynamoDB Client
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		fmt.Printf("unable to load SDK config, %v\n", err)
		os.Exit(1)
	}
	dbClient := dynamodb.NewFromConfig(cfg)

	//------------------------------------------------------------------------------
	// DynamoDB Table Tests
	//------------------------------------------------------------------------------

	// Set up a new DynamoDB Table, creating it if needed
	fmt.Println("Running the DynamoDB Table tests ...")
	dbTable := NewTable(dbClient, "test")
	if !dbTable.IsValid() {
		fmt.Println("invalid DynamoDB table definition", dbTable.TableName)
		os.Exit(1)
	}
	if !dbTable.TableExists(ctx) {
		err = dbTable.CreateTable(ctx)
		if err != nil {
			fmt.Printf("table %s creation failed: %v\n", dbTable.TableName, err)
			os.Exit(1)
		}
	}
	dbService := Service{
		EntityType: dbTable.EntityType,
		Table:      dbTable,
	}

	// Initialize the DynamoDB test database with sample data and run reader tests
	initDatabase(ctx, dbService)
	service = dbService
	exitVal := m.Run()

	// Run the deletion tests
	err = testDeletions(ctx, dbService)
	if err != nil {
		fmt.Printf("DynamoDB: %v\n", err)
	}

	// Delete the temporary testing table if it exists
	if dbTable.TableExists(ctx) {
		err = dbTable.DeleteTable(ctx)
		if err != nil {
			fmt.Printf("table %s deletion failed: %v\n", dbTable.TableName, err)
		}
	}

	// If tests failed, don't bother with the in-memory tests
	if exitVal != 0 {
		os.Exit(exitVal)
	}

	//------------------------------------------------------------------------------
	// In-Memory Table Tests
	//------------------------------------------------------------------------------

	// Set up an in-memory mock Table and Service for testing
	fmt.Println("Running the MemTable tests ...")
	memTable := NewMemTable(dbTable)
	if !memTable.IsValid() {
		fmt.Println("invalid in-memory table definition", memTable.TableName)
		os.Exit(1)
	}
	memService := Service{
		EntityType: memTable.EntityType,
		Table:      memTable,
	}

	// Initialize the in-memory test database with sample data and run reader tests
	initDatabase(ctx, memService)
	service = memService
	exitVal = m.Run()
	if exitVal != 0 {
		os.Exit(exitVal)
	}

	// Run the deletion tests
	err = testDeletions(ctx, memService)
	if err != nil {
		fmt.Printf("MemTable: %v\n", err)
	}
}

// Initialize the test database with sample data
func initDatabase(ctx context.Context, service Service) {
	var problems []string
	var err error
	now := time.Now()

	// Thing 1, Version 1.0
	v10, problems, err = service.Create(ctx, Thing{
		ID:      tuid.NewIDWithTime(now.AddDate(0, 0, -2)).String(),
		Message: "zyx v1.0",
		Count:   3,
		Tags:    []string{"tag1", "tag2"},
	})
	if len(problems) > 0 {
		fmt.Printf("v1.0 validation issues: %s\n", strings.Join(problems, ", "))
	}
	if err != nil {
		fmt.Printf("v1.0 create failed: %v\n", err)
		os.Exit(1)
	}

	// Thing 2, Version 2.0
	v20, problems, err = service.Create(ctx, Thing{
		ID:      tuid.NewIDWithTime(now.AddDate(0, 0, -1)).String(),
		Message: "abc v2.0",
		Count:   1,
		Tags:    []string{"tag1", "tag2"},
	})
	if len(problems) > 0 {
		fmt.Printf("v2.0 validation issues: %s\n", strings.Join(problems, ", "))
	}
	if err != nil {
		fmt.Printf("v2.0 create failed: %v\n", err)
		os.Exit(1)
	}

	// Thing 3, Version 3.0
	v30, problems, err = service.Create(ctx, Thing{
		ID:      tuid.NewIDWithTime(now).String(),
		Message: "lmn v3.0",
	})
	if len(problems) > 0 {
		fmt.Printf("v3.0 validation issues: %s\n", strings.Join(problems, ", "))
	}
	if err != nil {
		fmt.Printf("v3.0 create failed: %v\n", err)
		os.Exit(1)
	}

	// Thing 1, Version 1.1
	v11, problems, err = service.Update(ctx, Thing{
		ID:        v10.ID,
		CreatedAt: v10.CreatedAt,
		Message:   "zyx v1.1",
		Count:     2,
		Tags:      []string{"tag1", "tag3"},
	})
	if len(problems) > 0 {
		fmt.Printf("v1.1 validation issues: %s\n", strings.Join(problems, ", "))
	}
	if err != nil {
		fmt.Printf("v1.1 update failed: %v\n", err)
		os.Exit(1)
	}
}

func testDeletions(ctx context.Context, service Service) error {
	// Delete an entity, along with its entire revision history
	_, err := service.Delete(ctx, v30.ID)
	if err != nil {
		fmt.Printf("v3.0 delete failed: %v\n", err)
	}

	// Delete the current version
	v11Deleted, err := service.DeleteVersion(ctx, v11.ID, v11.VersionID)
	if err != nil {
		return fmt.Errorf("v1.1 delete version failed: %v", err)
	}

	// Expect the result to be the deleted version
	if v11Deleted.Message != v11.Message {
		return fmt.Errorf("v1.1 delete version failed: expected %s, got %s",
			v11.Message, v11Deleted.Message)
	}

	// Expect the new current version to be the previous version
	v10Current, err := service.Read(ctx, v11.ID)
	if err != nil {
		return fmt.Errorf("v1.1 delete version read current failed: %v", err)
	}
	if v10Current.Message != v10.Message {
		return fmt.Errorf("v1.1 delete version read current failed: expected %s, got %s",
			v10.Message, v10Current.Message)
	}

	// Expect the index row to be rolled back to the previous version
	msgs, err := service.ReadAllMessagesByTag(ctx, "tag2", true)
	if err != nil {
		return fmt.Errorf("v1.1 delete version read messages failed: %v", err)
	}
	if len(msgs) == 0 {
		return fmt.Errorf("v1.1 delete version read messages failed: expected value(s), got %d", len(msgs))
	}
	if msgs[len(msgs)-1].Value != v10.Message {
		return fmt.Errorf("v1.1 delete version read text values failed: expected %s, got %s",
			v10.Message, msgs[len(msgs)-1].Value)
	}
	return nil
}

func TestExists(t *testing.T) {
	expect := assert.New(t)
	expect.True(service.Exists(ctx, v11.ID))
	expect.False(service.Exists(ctx, tuid.NewID().String()))
	expect.False(service.Exists(ctx, ""))
}

func TestRead(t *testing.T) {
	expect := assert.New(t)
	thing, err := service.Read(ctx, v11.ID)
	if expect.NoError(err) {
		expect.Equal(v11, thing)
	}
	_, err = service.Read(ctx, tuid.NewID().String())
	if expect.Error(err) {
		expect.Equal(v.ErrNotFound, err)
	}

}

func TestReadAsJSON(t *testing.T) {
	expect := assert.New(t)
	j, err := service.ReadAsJSON(ctx, v11.ID)
	if expect.NoError(err) {
		expect.NotEmpty(j)
		expect.Contains(string(j), `"id":"`+v11.ID+`"`)
	}
}

func TestVersionExists(t *testing.T) {
	expect := assert.New(t)
	expect.True(service.VersionExists(ctx, v11.ID, v11.VersionID))
	expect.False(service.VersionExists(ctx, v11.ID, tuid.NewID().String()))
	expect.False(service.VersionExists(ctx, tuid.NewID().String(), v11.VersionID))
	expect.False(service.VersionExists(ctx, "", ""))
}

func TestReadVersion(t *testing.T) {
	expect := assert.New(t)
	thing, err := service.ReadVersion(ctx, v11.ID, v11.VersionID)
	if expect.NoError(err) {
		expect.Equal(v11, thing)
	}
	_, err = service.ReadVersion(ctx, v11.ID, tuid.NewID().String())
	if expect.Error(err) {
		expect.Equal(v.ErrNotFound, err)
	}
}

func TestReadVersionAsJSON(t *testing.T) {
	expect := assert.New(t)
	j, err := service.ReadVersionAsJSON(ctx, v11.ID, v11.VersionID)
	if expect.NoError(err) {
		expect.NotEmpty(j)
		expect.Contains(string(j), `"id":"`+v11.ID+`"`)
	}
	_, err = service.ReadVersionAsJSON(ctx, v11.ID, tuid.NewID().String())
	if expect.Error(err) {
		expect.Equal(v.ErrNotFound, err)
	}
}

func TestReadVersions(t *testing.T) {
	expect := assert.New(t)
	things, err := service.ReadVersions(ctx, v11.ID, false, 10, "")
	if expect.NoError(err) && expect.Equal(2, len(things)) {
		expect.Equal(v10, things[0])
		expect.Equal(v11, things[1])
	}
	things, err = service.ReadVersions(ctx, tuid.NewID().String(), false, 10, "")
	if expect.NoError(err) {
		expect.Equal(0, len(things))
	}
}

func TestReadVersionsAsJSON(t *testing.T) {
	expect := assert.New(t)
	j, err := service.ReadVersionsAsJSON(ctx, v11.ID, false, 10, "")
	if expect.NoError(err) {
		expect.NotEmpty(j)
		expect.Contains(string(j), `"id":"`+v11.ID+`"`)
	}
	j, err = service.ReadVersionsAsJSON(ctx, tuid.NewID().String(), false, 10, "")
	if expect.NoError(err) {
		expect.Equal("[]", string(j))
	}
}

func TestReadAllVersions(t *testing.T) {
	expect := assert.New(t)
	things, err := service.ReadAllVersions(ctx, v11.ID)
	if expect.NoError(err) && expect.Equal(2, len(things)) {
		expect.Equal(v10, things[0])
		expect.Equal(v11, things[1])
	}
	things, err = service.ReadAllVersions(ctx, tuid.NewID().String())
	if expect.NoError(err) {
		expect.Equal(0, len(things))
	}
}

func TestReadAllVersionsAsJSON(t *testing.T) {
	expect := assert.New(t)
	j, err := service.ReadAllVersionsAsJSON(ctx, v11.ID)
	if expect.NoError(err) {
		expect.NotEmpty(j)
		expect.Contains(string(j), `"id":"`+v11.ID+`"`)
	}
	j, err = service.ReadAllVersionsAsJSON(ctx, tuid.NewID().String())
	if expect.NoError(err) {
		expect.Equal("[]", string(j))
	}
}

func TestCountThings(t *testing.T) {
	expect := assert.New(t)
	count, err := service.CountThings(ctx)
	if expect.NoError(err) {
		expect.Equal(int64(3), count)
	}
}

func TestReadThingIDs(t *testing.T) {
	expect := assert.New(t)
	ids, err := service.ReadThingIDs(ctx, false, 10, "")
	if expect.NoError(err) && expect.Equal(3, len(ids)) {
		expect.Contains(ids, v10.ID)
		expect.Contains(ids, v20.ID)
		expect.Contains(ids, v30.ID)
	}
}

func TestReadAllThingIDs(t *testing.T) {
	expect := assert.New(t)
	ids, err := service.ReadAllThingIDs(ctx)
	if expect.NoError(err) && expect.Equal(3, len(ids)) {
		expect.Contains(ids, v10.ID)
		expect.Contains(ids, v20.ID)
		expect.Contains(ids, v30.ID)
	}
}

func TestReadThingMessages(t *testing.T) {
	expect := assert.New(t)
	msgs, err := service.ReadThingMessages(ctx, false, 10, "")
	if expect.NoError(err) && expect.Equal(3, len(msgs)) {
		expect.Equal(v11.Message, msgs[0].Value)
		expect.Equal(v20.Message, msgs[1].Value)
		expect.Equal(v30.Message, msgs[2].Value)
	}
}

func TestReadThingMessageRange(t *testing.T) {
	expect := assert.New(t)
	from := tuid.FirstIDWithTime(v11.CreatedAt).String()
	to := tuid.FirstIDWithTime(v30.CreatedAt).String()
	msgs, err := service.ReadThingMessageRange(ctx, from, to, false)
	if expect.NoError(err) && expect.Equal(2, len(msgs)) {
		expect.Equal(v11.Message, msgs[0].Value)
		expect.Equal(v20.Message, msgs[1].Value)
	}
	msgs, err = service.ReadThingMessageRange(ctx, from, to, true)
	if expect.NoError(err) && expect.Equal(2, len(msgs)) {
		expect.Equal(v20.Message, msgs[0].Value)
		expect.Equal(v11.Message, msgs[1].Value)
	}
	msgs, err = service.ReadThingMessageRange(ctx, to, "", false)
	if expect.NoError(err) && expect.Equal(1, len(msgs)) {
		expect.Equal(v30.Message, msgs[0].Value)
	}
	msgs, err = service.ReadThingMessageRange(ctx, "", "", true)
	if expect.NoError(err) && expect.Equal(3, len(msgs)) {
		expect.Equal(v30.Message, msgs[0].Value)
		expect.Equal(v20.Message, msgs[1].Value)
		expect.Equal(v11.Message, msgs[2].Value)
	}
	msgs, err = service.ReadThingMessageRange(ctx, "", "", false)
	if expect.NoError(err) && expect.Equal(3, len(msgs)) {
		expect.Equal(v11.Message, msgs[0].Value)
		expect.Equal(v20.Message, msgs[1].Value)
		expect.Equal(v30.Message, msgs[2].Value)
	}
}

func TestReadAllThingMessages(t *testing.T) {
	expect := assert.New(t)
	msgs, err := service.ReadAllThingMessages(ctx, true)
	if expect.NoError(err) && expect.Equal(3, len(msgs)) {
		expect.Contains(msgs[0].Value, "abc")
		expect.Contains(msgs[1].Value, "lmn")
		expect.Contains(msgs[2].Value, "zyx")
	}
}

func TestFilterThingMessages(t *testing.T) {
	expect := assert.New(t)
	msgs, err := service.FilterThingMessages(ctx, "ABC bogus", true)
	if expect.NoError(err) && expect.Equal(1, len(msgs)) {
		expect.Equal(v20.Message, msgs[0].Value)
	}
	msgs, err = service.FilterThingMessages(ctx, "abc bogus", false)
	if expect.NoError(err) {
		expect.Equal(0, len(msgs))
	}
}

func TestReadDates(t *testing.T) {
	expect := assert.New(t)
	dates, err := service.ReadDates(ctx, false, 10, "")
	if expect.NoError(err) {
		expect.Equal(3, len(dates))
		expect.Contains(dates, v10.CreatedOn())
		expect.Contains(dates, v20.CreatedOn())
		expect.Contains(dates, v30.CreatedOn())
	}
	dates, err = service.ReadDates(ctx, true, 1, "")
	if expect.NoError(err) {
		expect.Equal(1, len(dates))
		expect.Contains(dates, v30.CreatedOn())
	}
}

func TestReadAllDates(t *testing.T) {
	expect := assert.New(t)
	dates, err := service.ReadAllDates(ctx)
	if expect.NoError(err) {
		expect.Equal(3, len(dates))
		expect.Contains(dates, v10.CreatedOn())
		expect.Contains(dates, v20.CreatedOn())
		expect.Contains(dates, v30.CreatedOn())
	}
}

func TestReadThingsByDate(t *testing.T) {
	expect := assert.New(t)
	things, err := service.ReadThingsByDate(ctx, v11.CreatedOn(), false, 10, "")
	if expect.NoError(err) && expect.Equal(1, len(things)) {
		expect.Equal(v11, things[0])
	}
	things, err = service.ReadThingsByDate(ctx, v30.CreatedOn(), true, 10, "")
	if expect.NoError(err) && expect.Equal(1, len(things)) {
		expect.Equal(v30, things[0])
	}
	_, err = service.ReadThingsByDate(ctx, "bogus", false, 10, "")
	expect.Error(err)
	expect.Contains(err.Error(), "invalid date")
}

func TestReadThingsByDateAsJSON(t *testing.T) {
	expect := assert.New(t)
	j, err := service.ReadThingsByDateAsJSON(ctx, v11.CreatedOn(), false, 10, "")
	if expect.NoError(err) && expect.NotEmpty(j) {
		expect.Contains(string(j), `"id":"`+v11.ID+`"`)
	}
	j, err = service.ReadThingsByDateAsJSON(ctx, v30.CreatedOn(), true, 10, "")
	if expect.NoError(err) && expect.NotEmpty(j) {
		expect.Contains(string(j), `"id":"`+v30.ID+`"`)
	}
	_, err = service.ReadThingsByDateAsJSON(ctx, "bogus", false, 10, "")
	if expect.Error(err) {
		expect.Contains(err.Error(), "invalid date")
	}
}

func TestReadAllThingsByDate(t *testing.T) {
	expect := assert.New(t)
	things, err := service.ReadAllThingsByDate(ctx, v11.CreatedOn())
	if expect.NoError(err) && expect.Equal(1, len(things)) {
		expect.Equal(v11, things[0])
	}
	things, err = service.ReadAllThingsByDate(ctx, v30.CreatedOn())
	if expect.NoError(err) && expect.Equal(1, len(things)) {
		expect.Equal(v30, things[0])
	}
	_, err = service.ReadAllThingsByDate(ctx, "bogus")
	if expect.Error(err) {
		expect.Contains(err.Error(), "invalid date")
	}
}

func TestReadAllThingsByDateAsJSON(t *testing.T) {
	expect := assert.New(t)
	j, err := service.ReadAllThingsByDateAsJSON(ctx, v11.CreatedOn())
	if expect.NoError(err) && expect.NotEmpty(j) {
		expect.Contains(string(j), `"id":"`+v11.ID+`"`)
	}
	j, err = service.ReadAllThingsByDateAsJSON(ctx, v30.CreatedOn())
	if expect.NoError(err) && expect.NotEmpty(j) {
		expect.Contains(string(j), `"id":"`+v30.ID+`"`)
	}
	_, err = service.ReadAllThingsByDateAsJSON(ctx, "bogus")
	if expect.Error(err) {
		expect.Contains(err.Error(), "invalid date")
	}
}

func TestReadTags(t *testing.T) {
	expect := assert.New(t)
	tags, err := service.ReadTags(ctx, false, 10, "")
	if expect.NoError(err) {
		expect.Equal(3, len(tags))
		expect.Contains(tags, "tag1")
		expect.Contains(tags, "tag2")
		expect.Contains(tags, "tag3")
	}
	tags, err = service.ReadTags(ctx, true, 1, "")
	if expect.NoError(err) {
		expect.Equal(1, len(tags))
		expect.Contains(tags, "tag3")
	}
}

func TestReadAllTags(t *testing.T) {
	expect := assert.New(t)
	tags, err := service.ReadAllTags(ctx)
	if expect.NoError(err) {
		expect.Equal(3, len(tags))
		expect.Contains(tags, "tag1")
		expect.Contains(tags, "tag2")
		expect.Contains(tags, "tag3")
	}
}

func TestReadThingsByTag(t *testing.T) {
	expect := assert.New(t)
	things, err := service.ReadThingsByTag(ctx, "tag1", false, 10, "")
	if expect.NoError(err) && expect.Equal(2, len(things)) {
		expect.Equal(v11, things[0])
		expect.Equal(v20, things[1])
	}
	things, err = service.ReadThingsByTag(ctx, "tag3", true, 10, "")
	if expect.NoError(err) && expect.Equal(1, len(things)) {
		expect.Equal(v11, things[0])
	}
	things, err = service.ReadThingsByTag(ctx, "bogus", false, 10, "")
	if expect.NoError(err) {
		expect.Equal(0, len(things))
	}
}

func TestReadThingsByTagAsJSON(t *testing.T) {
	expect := assert.New(t)
	j, err := service.ReadThingsByTagAsJSON(ctx, "tag1", false, 10, "")
	if expect.NoError(err) && expect.NotEmpty(j) {
		expect.Contains(string(j), `"id":"`+v11.ID+`"`)
	}
	j, err = service.ReadThingsByTagAsJSON(ctx, "tag3", true, 10, "")
	if expect.NoError(err) && expect.NotEmpty(j) {
		expect.Contains(string(j), `"id":"`+v11.ID+`"`)
	}
	j, err = service.ReadThingsByTagAsJSON(ctx, "bogus", false, 10, "")
	if expect.NoError(err) {
		expect.Equal(string(j), "[]")
	}
}

func TestReadAllThingsByTag(t *testing.T) {
	expect := assert.New(t)
	things, err := service.ReadAllThingsByTag(ctx, "tag2")
	if expect.NoError(err) && expect.Equal(1, len(things)) {
		expect.Equal(v20, things[0])
	}
	things, err = service.ReadAllThingsByTag(ctx, "tag3")
	if expect.NoError(err) && expect.Equal(1, len(things)) {
		expect.Equal(v11, things[0])
	}
	things, err = service.ReadAllThingsByTag(ctx, "bogus")
	if expect.NoError(err) {
		expect.Equal(0, len(things))
	}
}

func TestReadAllThingsByTagAsJSON(t *testing.T) {
	expect := assert.New(t)
	j, err := service.ReadAllThingsByTagAsJSON(ctx, "tag2")
	if expect.NoError(err) && expect.NotEmpty(j) {
		expect.Contains(string(j), `"id":"`+v20.ID+`"`)
	}
	j, err = service.ReadAllThingsByTagAsJSON(ctx, "tag3")
	if expect.NoError(err) && expect.NotEmpty(j) {
		expect.Contains(string(j), `"id":"`+v11.ID+`"`)
	}
	j, err = service.ReadAllThingsByTagAsJSON(ctx, "bogus")
	if expect.NoError(err) {
		expect.Equal(string(j), "[]")
	}
}

func TestReadMessagesByTag(t *testing.T) {
	expect := assert.New(t)
	msgs, err := service.ReadMessagesByTag(ctx, "tag1", false, 10, "")
	if expect.NoError(err) && expect.Equal(2, len(msgs)) {
		expect.Equal(v11.Message, msgs[0].Value)
		expect.Equal(v20.Message, msgs[1].Value)
	}
	msgs, err = service.ReadMessagesByTag(ctx, "tag3", true, 10, "")
	if expect.NoError(err) && expect.Equal(1, len(msgs)) {
		expect.Equal(v11.Message, msgs[0].Value)
	}
	msgs, err = service.ReadMessagesByTag(ctx, "bogus", false, 10, "")
	if expect.NoError(err) {
		expect.Equal(0, len(msgs))
	}
}

func TestReadMessageRangeByTag(t *testing.T) {
	expect := assert.New(t)
	from := tuid.FirstIDWithTime(v11.CreatedAt).String()
	to := tuid.FirstIDWithTime(v20.CreatedAt).String()
	msgs, err := service.ReadMessageRangeByTag(ctx, "tag1", from, to, false)
	if expect.NoError(err) && expect.Equal(1, len(msgs)) {
		expect.Equal(v11.Message, msgs[0].Value)
	}
	msgs, err = service.ReadMessageRangeByTag(ctx, "tag1", "", to, true)
	if expect.NoError(err) && expect.Equal(1, len(msgs)) {
		expect.Equal(v20.Message, msgs[0].Value)
	}
	msgs, err = service.ReadMessageRangeByTag(ctx, "bogus", from, to, false)
	if expect.NoError(err) {
		expect.Equal(0, len(msgs))
	}
}

func TestReadAllMessagesByTag(t *testing.T) {
	expect := assert.New(t)
	msgs, err := service.ReadAllMessagesByTag(ctx, "tag1", true)
	if expect.NoError(err) && expect.Equal(2, len(msgs)) {
		expect.Contains(msgs[0].Value, "abc")
		expect.Contains(msgs[1].Value, "zyx")
	}
	msgs, err = service.ReadAllMessagesByTag(ctx, "tag3", false)
	if expect.NoError(err) && expect.Equal(1, len(msgs)) {
		expect.Contains(msgs[0].Value, "zyx")
	}
	msgs, err = service.ReadAllMessagesByTag(ctx, "bogus", false)
	if expect.NoError(err) {
		expect.Equal(0, len(msgs))
	}
}

func TestFilterMessagesByTag(t *testing.T) {
	expect := assert.New(t)
	// case-insensitive, any terms match
	msgs, err := service.FilterMessagesByTag(ctx, "tag1", "ABC bogus", true)
	if expect.NoError(err) && expect.Equal(1, len(msgs)) {
		expect.Equal(v20.Message, msgs[0].Value)
	}
	// all terms must match
	msgs, err = service.FilterMessagesByTag(ctx, "tag1", "abc v2.0", false)
	if expect.NoError(err) && expect.Equal(1, len(msgs)) {
		expect.Equal(v20.Message, msgs[0].Value)
	}
	msgs, err = service.FilterMessagesByTag(ctx, "tag1", "abc bogus", false)
	if expect.NoError(err) {
		expect.Equal(0, len(msgs))
	}
	// invalid tag
	msgs, err = service.FilterMessagesByTag(ctx, "bogus", "abc", true)
	if expect.NoError(err) {
		expect.Equal(0, len(msgs))
	}
}

func TestReadCountsByTag(t *testing.T) {
	expect := assert.New(t)
	counts, err := service.ReadCountsByTag(ctx, "tag1", false, 10, "")
	if expect.NoError(err) && expect.Equal(2, len(counts)) {
		expect.Equal(float64(v11.Count), counts[0].Value)
		expect.Equal(float64(v20.Count), counts[1].Value)
	}
	counts, err = service.ReadCountsByTag(ctx, "tag3", true, 1, "")
	if expect.NoError(err) && expect.Equal(1, len(counts)) {
		expect.Equal(float64(v11.Count), counts[0].Value)
	}
	counts, err = service.ReadCountsByTag(ctx, "bogus", false, 10, "")
	if expect.NoError(err) {
		expect.Equal(0, len(counts))
	}
}

func TestReadAllCountsByTag(t *testing.T) {
	expect := assert.New(t)
	counts, err := service.ReadAllCountsByTag(ctx, "tag1", true)
	if expect.NoError(err) && expect.Equal(2, len(counts)) {
		expect.Equal(float64(v20.Count), counts[0].Value) // 1
		expect.Equal(float64(v11.Count), counts[1].Value) // 2
	}
	counts, err = service.ReadAllCountsByTag(ctx, "tag3", false)
	if expect.NoError(err) && expect.Equal(1, len(counts)) {
		expect.Equal(float64(v11.Count), counts[0].Value)
	}
	counts, err = service.ReadAllCountsByTag(ctx, "bogus", false)
	if expect.NoError(err) {
		expect.Equal(0, len(counts))
	}
}

func TestSumCountsByTag(t *testing.T) {
	expect := assert.New(t)
	sum, err := service.SumCountsByTag(ctx, "tag1")
	if expect.NoError(err) {
		expect.Equal(float64(3), sum)
	}
	sum, err = service.SumCountsByTag(ctx, "tag2")
	if expect.NoError(err) {
		expect.Equal(float64(1), sum)
	}
	sum, err = service.SumCountsByTag(ctx, "bogus")
	if expect.NoError(err) {
		expect.Equal(float64(0), sum)
	}
}

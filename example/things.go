package thing

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/voxtechnica/tuid-go"
	v "github.com/voxtechnica/versionary"
)

//==============================================================================
// Thing Table
//==============================================================================

// rowThingsVersion is a TableRow that contains all Thing versions,
// partitioned by ID and ordered by VersionID
var rowThingsVersion = v.TableRow[Thing]{
	RowName:      "things_version",
	PartKeyName:  "id",
	PartKeyValue: func(t Thing) string { return t.ID },
	PartKeyLabel: func(t Thing) string { return t.Message },
	SortKeyName:  "version_id",
	SortKeyValue: func(t Thing) string { return t.VersionID },
	JsonValue:    func(t Thing) []byte { return t.CompressedJSON() },
	TimeToLive:   func(t Thing) int64 { return t.ExpiresAt.Unix() },
}

// rowThingsDate is a TableRow that contains current Thing versions,
// partitioned by CreatedOn date and ordered by ID
var rowThingsDate = v.TableRow[Thing]{
	RowName:      "things_day",
	PartKeyName:  "day",
	PartKeyValue: func(t Thing) string { return t.CreatedOn() },
	SortKeyName:  "id",
	SortKeyValue: func(t Thing) string { return t.ID },
	JsonValue:    func(t Thing) []byte { return t.CompressedJSON() },
	TimeToLive:   func(t Thing) int64 { return t.ExpiresAt.Unix() },
}

// rowThingsTag is a TableRow that contains current Thing properties,
// partitioned by Tag and ordered by ID
var rowThingsTag = v.TableRow[Thing]{
	RowName:       "things_tag",
	PartKeyName:   "tag",
	PartKeyValues: func(t Thing) []string { return t.Tags },
	SortKeyName:   "id",
	SortKeyValue:  func(t Thing) string { return t.ID },
	JsonValue:     func(t Thing) []byte { return t.CompressedJSON() },
	TextValue:     func(t Thing) string { return t.Message },
	NumericValue:  func(t Thing) float64 { return float64(t.Count) },
	TimeToLive:    func(t Thing) int64 { return t.ExpiresAt.Unix() },
}

// NewTable returns a new Table for Thing objects
func NewTable(dbClient *dynamodb.Client, env string) v.Table[Thing] {
	if env == "" {
		env = "dev"
	}
	return v.Table[Thing]{
		Client:     dbClient,
		EntityType: "Thing",
		TableName:  "things" + "_" + env,
		TTL:        true,
		EntityRow:  rowThingsVersion,
		IndexRows: map[string]v.TableRow[Thing]{
			rowThingsDate.RowName: rowThingsDate,
			rowThingsTag.RowName:  rowThingsTag,
		},
	}
}

// NewMemTable creates an in-memory Thing table for testing purposes.
func NewMemTable(table v.Table[Thing]) v.MemTable[Thing] {
	return v.NewMemTable(table)
}

//==============================================================================
// Thing Service
//==============================================================================

// Service is used to manage a Thing database.
type Service struct {
	EntityType string
	Table      v.TableReadWriter[Thing]
}

// NewService returns a new Thing Service for the specified operating environment.
func NewService(dbClient *dynamodb.Client, env string) Service {
	table := NewTable(dbClient, env)
	return Service{
		EntityType: table.EntityType,
		Table:      table,
	}
}

// NewMemService returns a new Thing Service backed by an in-memory table for testing.
func NewMemService(env string) Service {
	table := NewMemTable(NewTable(nil, env))
	return Service{
		EntityType: table.EntityType,
		Table:      table,
	}
}

//------------------------------------------------------------------------------
// Thing Versions
//------------------------------------------------------------------------------

// Create a new Thing in the Thing table.
func (s Service) Create(ctx context.Context, t Thing) (Thing, []string, error) {
	if t.ID == "" || !tuid.IsValid(tuid.TUID(t.ID)) {
		t.ID = tuid.NewID().String()
	}
	id := tuid.TUID(t.ID)
	at, _ := id.Time()
	t.ID = id.String()
	t.VersionID = id.String()
	t.CreatedAt = at
	t.UpdatedAt = at
	t.ExpiresAt = at.AddDate(1, 0, 0)
	problems := t.Validate()
	if len(problems) > 0 {
		return t, problems, fmt.Errorf("error creating %s %s: invalid field(s): %s",
			s.EntityType, t.ID, strings.Join(problems, ", "))
	}
	err := s.Table.WriteEntity(ctx, t)
	if err != nil {
		return t, problems, fmt.Errorf("error creating %s %s: %s", s.EntityType, t.ID, err)
	}
	return t, problems, nil
}

// Update a Thing in the Thing table, creating a new version and ensuring that the
// index rows are consistent.
func (s Service) Update(ctx context.Context, t Thing) (Thing, []string, error) {
	id := tuid.NewID()
	at, _ := id.Time()
	t.VersionID = id.String()
	t.UpdatedAt = at
	t.ExpiresAt = at.AddDate(1, 0, 0)
	problems := t.Validate()
	if len(problems) > 0 {
		return t, problems, fmt.Errorf("error updating %s %s: invalid field(s): %s",
			s.EntityType, t.ID, strings.Join(problems, ", "))
	}
	err := s.Table.UpdateEntity(ctx, t)
	if err != nil {
		return t, problems, fmt.Errorf("error updating %s %s: %s", s.EntityType, t.ID, err)
	}
	return t, problems, nil
}

// Delete a Thing from the Thing table. The deleted Thing is returned.
func (s Service) Delete(ctx context.Context, id string) (Thing, error) {
	return s.Table.DeleteEntityWithID(ctx, id)
}

// Delete a Thing version from the Thing table. The deleted Thing is returned.
func (s Service) DeleteVersion(ctx context.Context, id, versionID string) (Thing, error) {
	return s.Table.DeleteEntityVersionWithID(ctx, id, versionID)
}

// Exists checks if a Thing exists in the Thing table.
func (s Service) Exists(ctx context.Context, id string) bool {
	return s.Table.EntityExists(ctx, id)
}

// Read a specified Thing from the Thing table.
func (s Service) Read(ctx context.Context, id string) (Thing, error) {
	return s.Table.ReadEntity(ctx, id)
}

// ReadAsJSON gets a specified Thing from the Thing table, serialized as JSON.
func (s Service) ReadAsJSON(ctx context.Context, id string) ([]byte, error) {
	return s.Table.ReadEntityAsJSON(ctx, id)
}

// VersionExists checks if a specified Thing version exists in the Thing table.
func (s Service) VersionExists(ctx context.Context, id, versionID string) bool {
	return s.Table.EntityVersionExists(ctx, id, versionID)
}

// ReadVersion gets a specified Thing version from the Thing table.
func (s Service) ReadVersion(ctx context.Context, id, versionID string) (Thing, error) {
	return s.Table.ReadEntityVersion(ctx, id, versionID)
}

// ReadVersionAsJSON gets a specified Thing version from the Thing table, serialized as JSON.
func (s Service) ReadVersionAsJSON(ctx context.Context, id, versionID string) ([]byte, error) {
	return s.Table.ReadEntityVersionAsJSON(ctx, id, versionID)
}

// ReadVersions returns paginated versions of the specified Thing.
// Sorting is chronological (or reverse). The offset is the last ID returned in a previous request.
func (s Service) ReadVersions(ctx context.Context, id string, reverse bool, limit int, offset string) ([]Thing, error) {
	return s.Table.ReadEntityVersions(ctx, id, reverse, limit, offset)
}

// ReadVersionsAsJSON returns paginated versions of the specified Thing, serialized as JSON.
// Sorting is chronological (or reverse). The offset is the last ID returned in a previous request.
func (s Service) ReadVersionsAsJSON(ctx context.Context, id string, reverse bool, limit int, offset string) ([]byte, error) {
	return s.Table.ReadEntityVersionsAsJSON(ctx, id, reverse, limit, offset)
}

// ReadAllVersions returns all versions of the specified Thing in chronological order.
// Caution: this may be a LOT of data!
func (s Service) ReadAllVersions(ctx context.Context, id string) ([]Thing, error) {
	return s.Table.ReadAllEntityVersions(ctx, id)
}

// ReadAllVersionsAsJSON returns all versions of the specified Thing, serialized as JSON.
// Caution: this may be a LOT of data!
func (s Service) ReadAllVersionsAsJSON(ctx context.Context, id string) ([]byte, error) {
	return s.Table.ReadAllEntityVersionsAsJSON(ctx, id)
}

// CountThings returns the number of Things in the Thing table.
func (s Service) CountThings(ctx context.Context) (int64, error) {
	return s.Table.CountPartKeyValues(ctx, rowThingsVersion)
}

// ReadThingIDs returns a paginated list of Thing IDs in the Thing table.
// Sorting is chronological (or reverse). The offset is the last ID returned in a previous request.
func (s Service) ReadThingIDs(ctx context.Context, reverse bool, limit int, offset string) ([]string, error) {
	return s.Table.ReadEntityIDs(ctx, reverse, limit, offset)
}

// ReadAllThingIDs returns a complete chronological list of all Thing IDs in the Thing table.
func (s Service) ReadAllThingIDs(ctx context.Context) ([]string, error) {
	return s.Table.ReadAllEntityIDs(ctx)
}

// ReadThingMessages returns a paginated list of Thing IDs and messages in the Thing table.
// Sorting is chronological (or reverse). The offset is the last ID returned in a previous request.
func (s Service) ReadThingMessages(ctx context.Context, reverse bool, limit int, offset string) ([]v.TextValue, error) {
	return s.Table.ReadEntityLabels(ctx, reverse, limit, offset)
}

// ReadThingMessageRange returns a range of Thing IDs with associated messages.
// The 'from' and 'to' parameters are inclusive. If omitted, they're replaced with sentinel values.
func (s Service) ReadThingMessageRange(ctx context.Context, from, to string, reverse bool) ([]v.TextValue, error) {
	return s.Table.ReadEntityLabelRange(ctx, from, to, reverse)
}

// ReadAllThingMessages returns all Thing IDs and messages in the Thing table,
// optionally sorted by message (the default is sorted by ID).
// Caution: this may be a LOT of data!
func (s Service) ReadAllThingMessages(ctx context.Context, sortByValue bool) ([]v.TextValue, error) {
	return s.Table.ReadAllEntityLabels(ctx, sortByValue)
}

// FilterThingMessages returns a filtered list of Thing IDs and messages.
// The case-insensitive contains query is split into words, and the words are compared with the message.
// If anyMatch is true, then a TextValue is included in the results if any of the words are found (OR filter).
// If anyMatch is false, then the TextValue must contain all the words in the query string (AND filter).
// The filtered results are sorted alphabetically by message, not by ID.
func (s Service) FilterThingMessages(ctx context.Context, contains string, anyMatch bool) ([]v.TextValue, error) {
	filter, err := v.ContainsFilter(contains, anyMatch)
	if err != nil {
		return []v.TextValue{}, err
	}
	return s.Table.FilterEntityLabels(ctx, filter)
}

//------------------------------------------------------------------------------
// Things by Date (YYYY-MM-DD)
//------------------------------------------------------------------------------

// ReadDates returns a paginated list of days for which there are Things in the table.
// Sorting is chronological (or reverse). The offset is the last day returned in a previous request.
func (s Service) ReadDates(ctx context.Context, reverse bool, limit int, offset string) ([]string, error) {
	return s.Table.ReadPartKeyValues(ctx, rowThingsDate, reverse, limit, offset)
}

// ReadAllDates returns a complete chronological list of all days for which there are Things in the table.
func (s Service) ReadAllDates(ctx context.Context) ([]string, error) {
	return s.Table.ReadAllPartKeyValues(ctx, rowThingsDate)
}

// ReadThingsByDate returns a paginated list of Things for a specified day.
// Sorting is chronological (or reverse). The offset is the last ID returned in a previous request.
func (s Service) ReadThingsByDate(ctx context.Context, day string, reverse bool, limit int, offset string) ([]Thing, error) {
	if !v.IsValidDate(day) {
		return []Thing{}, fmt.Errorf("invalid date format (expect YYYY-MM-DD): %s", day)
	}
	return s.Table.ReadEntitiesFromRow(ctx, rowThingsDate, day, reverse, limit, offset)
}

// ReadThingsByDateAsJSON returns a paginated list of Things for a specified day, serialized as JSON.
// Sorting is chronological (or reverse). The offset is the last ID returned in a previous request.
func (s Service) ReadThingsByDateAsJSON(ctx context.Context, day string, reverse bool, limit int, offset string) ([]byte, error) {
	if !v.IsValidDate(day) {
		return nil, fmt.Errorf("invalid date format (expect YYYY-MM-DD): %s", day)
	}
	return s.Table.ReadEntitiesFromRowAsJSON(ctx, rowThingsDate, day, reverse, limit, offset)
}

// ReadAllThingsByDate returns a complete chronological list of all Things for a specified day.
func (s Service) ReadAllThingsByDate(ctx context.Context, day string) ([]Thing, error) {
	if !v.IsValidDate(day) {
		return []Thing{}, fmt.Errorf("invalid date format (expect YYYY-MM-DD): %s", day)
	}
	return s.Table.ReadAllEntitiesFromRow(ctx, rowThingsDate, day)
}

// ReadAllThingsByDateAsJSON returns a complete chronological list of all Things for a specified day, serialized as JSON.
func (s Service) ReadAllThingsByDateAsJSON(ctx context.Context, day string) ([]byte, error) {
	if !v.IsValidDate(day) {
		return nil, fmt.Errorf("invalid date format (expect YYYY-MM-DD): %s", day)
	}
	return s.Table.ReadAllEntitiesFromRowAsJSON(ctx, rowThingsDate, day)
}

//------------------------------------------------------------------------------
// Thing properties by Tag
//------------------------------------------------------------------------------

// ReadTags returns a paginated list of tags for which there are Things in the table.
// Sorting is alphabetical (or reverse). The offset is the last tag returned in a previous request.
func (s Service) ReadTags(ctx context.Context, reverse bool, limit int, offset string) ([]string, error) {
	return s.Table.ReadPartKeyValues(ctx, rowThingsTag, reverse, limit, offset)
}

// ReadAllTags returns a complete alphabetical list of all tags for which there are Things in the table.
func (s Service) ReadAllTags(ctx context.Context) ([]string, error) {
	return s.Table.ReadAllPartKeyValues(ctx, rowThingsTag)
}

// ReadThingsByTag returns a paginated list of Things for a specified tag.
// Sorting is chronological (or reverse). The offset is the last ID returned in a previous request.
func (s Service) ReadThingsByTag(ctx context.Context, tag string, reverse bool, limit int, offset string) ([]Thing, error) {
	return s.Table.ReadEntitiesFromRow(ctx, rowThingsTag, tag, reverse, limit, offset)
}

// ReadThingsByTagAsJSON returns a paginated list of Things for a specified tag, serialized as JSON.
// Sorting is chronological (or reverse). The offset is the last ID returned in a previous request.
func (s Service) ReadThingsByTagAsJSON(ctx context.Context, tag string, reverse bool, limit int, offset string) ([]byte, error) {
	return s.Table.ReadEntitiesFromRowAsJSON(ctx, rowThingsTag, tag, reverse, limit, offset)
}

// ReadAllThingsByTag returns a complete chronological list of all Things for a specified tag.
func (s Service) ReadAllThingsByTag(ctx context.Context, tag string) ([]Thing, error) {
	return s.Table.ReadAllEntitiesFromRow(ctx, rowThingsTag, tag)
}

// ReadAllThingsByTagAsJSON returns a complete chronological list of all Things for a specified tag, serialized as JSON.
func (s Service) ReadAllThingsByTagAsJSON(ctx context.Context, tag string) ([]byte, error) {
	return s.Table.ReadAllEntitiesFromRowAsJSON(ctx, rowThingsTag, tag)
}

// ReadMessagesByTag returns a paginated list of Thing IDs and messages for a specified tag.
// Sorting is chronological (or reverse). The offset is the last ID returned in a previous request.
func (s Service) ReadMessagesByTag(ctx context.Context, tag string, reverse bool, limit int, offset string) ([]v.TextValue, error) {
	return s.Table.ReadTextValues(ctx, rowThingsTag, tag, reverse, limit, offset)
}

// ReadMessageRangeByTag returns a range of Thing IDs with associated messages for a specified tag.
// The 'from' and 'to' parameters are inclusive. If omitted, they're replaced with sentinel values.
func (s Service) ReadMessageRangeByTag(ctx context.Context, tag string, from, to string, reverse bool) ([]v.TextValue, error) {
	return s.Table.ReadTextValueRange(ctx, rowThingsTag, tag, from, to, reverse)
}

// ReadAllMessagesByTag returns a complete list of Thing IDs and messages for a specified tag,
// optionally sorted by message. The default order is chronological (sorted by ID).
func (s Service) ReadAllMessagesByTag(ctx context.Context, tag string, sortByValue bool) ([]v.TextValue, error) {
	return s.Table.ReadAllTextValues(ctx, rowThingsTag, tag, sortByValue)
}

// FilterMessagesByTag returns a filtered list of Thing IDs and messages for a specified tag.
// The case-insensitive contains query is split into words, and the words are compared with the value in the TextValue.
// If anyMatch is true, then a TextValue is included in the results if any of the words are found (OR filter).
// If anyMatch is false, then the TextValue must contain all the words in the query string (AND filter).
// The filtered results are sorted alphabetically by value, not by ID.
func (s Service) FilterMessagesByTag(ctx context.Context, tag string, contains string, anyMatch bool) ([]v.TextValue, error) {
	filter, err := v.ContainsFilter(contains, anyMatch)
	if err != nil {
		return []v.TextValue{}, err
	}
	return s.Table.FilterTextValues(ctx, rowThingsTag, tag, filter)
}

// ReadCountsByTag returns a paginated list of Thing IDs and counts for a specified tag.
// Sorting is chronological (or reverse). The offset is the last ID returned in a previous request.
func (s Service) ReadCountsByTag(ctx context.Context, tag string, reverse bool, limit int, offset string) ([]v.NumValue, error) {
	return s.Table.ReadNumericValues(ctx, rowThingsTag, tag, reverse, limit, offset)
}

// ReadAllCountsByTag returns a complete list of Thing IDs and counts for a specified tag,
// optionally sorted by ascending count. The default order is chronological (sorted by ID).
func (s Service) ReadAllCountsByTag(ctx context.Context, tag string, sortByValue bool) ([]v.NumValue, error) {
	return s.Table.ReadAllNumericValues(ctx, rowThingsTag, tag, sortByValue)
}

// SumCountsByTag returns the sum of all counts for a specified tag.
func (s Service) SumCountsByTag(ctx context.Context, tag string) (float64, error) {
	values, err := s.ReadAllCountsByTag(ctx, tag, false)
	if err != nil {
		return 0, err
	}
	sum := v.Reduce[v.NumValue, float64](values, 0,
		func(s float64, n v.NumValue) float64 { return s + n.Value })
	return sum, nil
}

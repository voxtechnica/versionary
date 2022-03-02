package versionary

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"sort"
	"strconv"
)

// TextValue represents a key-value pair where the value is a string.
type TextValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// NumValue represents a key-value pair where the value is a number.
type NumValue struct {
	Key   string  `json:"key"`
	Value float64 `json:"value"`
}

// Record is a struct that represents a single item in a database table. Although DynamoDB AttributeValue struct tags
// are provided for marshalling and unmarshalling, using ToItem() and FromItem() are preferred because they are much
// less computationally intensive.
type Record struct {
	PartKeyValue string  `dynamodbav:"v_part"`
	SortKeyValue string  `dynamodbav:"v_sort"`
	JsonValue    []byte  `dynamodbav:"v_json,omitempty"`
	TextValue    string  `dynamodbav:"v_text,omitempty"`
	NumericValue float64 `dynamodbav:"v_num,omitempty"`
	TimeToLive   int64   `dynamodbav:"v_expires,omitempty"`
}

// IsValid returns true if the Record is valid (all required fields are supplied).
func (r *Record) IsValid() bool {
	return r.PartKeyValue != "" && r.SortKeyValue != ""
}

// FromItem converts a DynamoDB AttributeValue map (an "item") to a Record.
func (r *Record) FromItem(item map[string]types.AttributeValue) {
	// Zero out the struct in preparation for new values (in case it's being reused).
	r.PartKeyValue = ""
	r.SortKeyValue = ""
	r.JsonValue = nil
	r.TextValue = ""
	r.NumericValue = 0
	r.TimeToLive = 0
	if item == nil {
		return
	}
	if v, ok := item["v_part"]; ok {
		r.PartKeyValue = v.(*types.AttributeValueMemberS).Value
	}
	if v, ok := item["v_sort"]; ok {
		r.SortKeyValue = v.(*types.AttributeValueMemberS).Value
	}
	if v, ok := item["v_json"]; ok {
		r.JsonValue = v.(*types.AttributeValueMemberB).Value
	}
	if v, ok := item["v_text"]; ok {
		r.TextValue = v.(*types.AttributeValueMemberS).Value
	}
	if v, ok := item["v_num"]; ok {
		r.NumericValue, _ = strconv.ParseFloat(v.(*types.AttributeValueMemberN).Value, 64)
	}
	if v, ok := item["v_expires"]; ok {
		r.TimeToLive, _ = strconv.ParseInt(v.(*types.AttributeValueMemberN).Value, 10, 64)
	}
}

// ToItem converts a Record to a DynamoDB AttributeValue map (an "item").
func (r *Record) ToItem() map[string]types.AttributeValue {
	item := map[string]types.AttributeValue{}
	if r.PartKeyValue != "" {
		item["v_part"] = &types.AttributeValueMemberS{Value: r.PartKeyValue}
	}
	if r.SortKeyValue != "" {
		item["v_sort"] = &types.AttributeValueMemberS{Value: r.SortKeyValue}
	}
	if r.JsonValue != nil {
		item["v_json"] = &types.AttributeValueMemberB{Value: r.JsonValue}
	}
	if r.TextValue != "" {
		item["v_text"] = &types.AttributeValueMemberS{Value: r.TextValue}
	}
	if r.NumericValue != 0 {
		item["v_num"] = &types.AttributeValueMemberN{Value: strconv.FormatFloat(r.NumericValue, 'f', -1, 64)}
	}
	if r.TimeToLive != 0 {
		item["v_expires"] = &types.AttributeValueMemberN{Value: strconv.FormatInt(r.TimeToLive, 10)}
	}
	return item
}

// ToKey converts a Record to a DynamoDB AttributeValue map (an "item") for hash and range keys.
func (r *Record) ToKey() map[string]types.AttributeValue {
	item := map[string]types.AttributeValue{}
	if r.PartKeyValue != "" {
		item["v_part"] = &types.AttributeValueMemberS{Value: r.PartKeyValue}
	}
	if r.SortKeyValue != "" {
		item["v_sort"] = &types.AttributeValueMemberS{Value: r.SortKeyValue}
	}
	return item
}

// ToTextValue extracts the text value from a Record.
func (r *Record) ToTextValue() TextValue {
	return TextValue{Key: r.SortKeyValue, Value: r.TextValue}
}

// ToNumValue extracts the numeric value from a Record.
func (r *Record) ToNumValue() NumValue {
	return NumValue{Key: r.SortKeyValue, Value: r.NumericValue}
}

// PutRequest converts the Record to a DynamoDB WriteRequest containing a PutRequest.
func (r *Record) PutRequest() types.WriteRequest {
	return types.WriteRequest{PutRequest: &types.PutRequest{Item: r.ToItem()}}
}

// DeleteRequest converts the Record to a DynamoDB WriteRequest containing a DeleteRequest.
func (r *Record) DeleteRequest() types.WriteRequest {
	return types.WriteRequest{DeleteRequest: &types.DeleteRequest{Key: r.ToKey()}}
}

// RecordSet provides an in-memory data structure for storing a set of Records, used for lightweight testing purposes.
type RecordSet map[string]map[string]Record

// SetRecord adds a Record to the RecordSet.
func (rs *RecordSet) SetRecord(r Record) {
	if *rs == nil {
		*rs = make(RecordSet)
	}
	if (*rs)[r.PartKeyValue] == nil {
		(*rs)[r.PartKeyValue] = make(map[string]Record)
	}
	(*rs)[r.PartKeyValue][r.SortKeyValue] = r
}

// SetRecords adds a list of Records to the RecordSet.
func (rs *RecordSet) SetRecords(records []Record) {
	if *rs == nil {
		*rs = make(RecordSet)
	}
	for _, record := range records {
		(*rs).SetRecord(record)
	}
}

// GetRecord returns a specified Record from the RecordSet.
func (rs *RecordSet) GetRecord(partKey string, sortKey string) (Record, bool) {
	if *rs == nil || (*rs)[partKey] == nil {
		return Record{}, false
	}
	record, ok := (*rs)[partKey][sortKey]
	return record, ok
}

// GetRecords returns a list of Records from the RecordSet.
func (rs *RecordSet) GetRecords(partKey string, sortKeys []string) []Record {
	var records []Record
	if *rs == nil || (*rs)[partKey] == nil {
		return records
	}
	for _, sortKey := range sortKeys {
		record, ok := (*rs)[partKey][sortKey]
		if ok {
			records = append(records, record)
		}
	}
	return records
}

// GetSortKeys returns a complete list of sort keys for a specified partition key.
func (rs *RecordSet) GetSortKeys(partKey string) []string {
	if *rs == nil || (*rs)[partKey] == nil {
		return []string{}
	}
	sortKeys := make([]string, 0, len((*rs)[partKey]))
	for sortKey := range (*rs)[partKey] {
		sortKeys = append(sortKeys, sortKey)
	}
	sort.Strings(sortKeys)
	return sortKeys
}

// DeleteRecordForKeys removes a specified Record from the RecordSet.
func (rs *RecordSet) DeleteRecordForKeys(partKey string, sortKey string) {
	if *rs != nil && (*rs)[partKey] != nil {
		delete((*rs)[partKey], sortKey)
	}
}

// DeleteRecordsForKey removes all Records for a specified partition key from the RecordSet.
func (rs *RecordSet) DeleteRecordsForKey(partKey string) {
	if *rs != nil {
		delete(*rs, partKey)
	}
}

// DeleteRecord removes the provided Record from the RecordSet.
func (rs *RecordSet) DeleteRecord(r Record) {
	if *rs != nil && (*rs)[r.PartKeyValue] != nil {
		delete((*rs)[r.PartKeyValue], r.SortKeyValue)
	}
}

// DeleteRecords removes the provided list of Records from the RecordSet.
func (rs *RecordSet) DeleteRecords(records []Record) {
	if *rs == nil {
		return
	}
	for _, record := range records {
		(*rs).DeleteRecord(record)
	}
}

// RecordsExist returns true if the RecordSet contains any records for the provided partition key.
func (rs *RecordSet) RecordsExist(partKey string) bool {
	if *rs == nil {
		return false
	}
	records, ok := (*rs)[partKey]
	return ok && len(records) > 0
}

// RecordExists returns true if the RecordSet contains a record for the provided partition and sort key.
func (rs *RecordSet) RecordExists(partKey string, sortKey string) bool {
	if *rs == nil || (*rs)[partKey] == nil {
		return false
	}
	_, ok := (*rs)[partKey][sortKey]
	return ok
}

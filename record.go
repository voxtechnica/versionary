package versionary

import (
	"sort"
	"strings"
)

// TextValue represents a key-value pair where the value is a string.
type TextValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// ContainsAny returns true if the Value contains any of the terms (an OR filter).
// The terms should be lowercase for a case-insensitive search.
func (tv TextValue) ContainsAny(terms []string) bool {
	v := strings.ToLower(tv.Value)
	for _, t := range terms {
		if strings.Contains(v, t) {
			return true
		}
	}
	return false
}

// ContainsAll returns true if the Value contains all the terms (an AND filter).
// The terms should be lowercase for a case-insensitive search.
func (tv TextValue) ContainsAll(terms []string) bool {
	v := strings.ToLower(tv.Value)
	for _, t := range terms {
		if !strings.Contains(v, t) {
			return false
		}
	}
	return true
}

// NumValue represents a key-value pair where the value is a number.
type NumValue struct {
	Key   string  `json:"key"`
	Value float64 `json:"value"`
}

// Record is a struct that represents a single item in a database table. PartKeyValue and SortKeyValue
// are used to represent the primary key and are required fields. All other fields are optional.
// Note that the PartKeyValue will be a full pipe-delimited partition key: rowName|partKeyName|partKeyValue.
type Record struct {
	PartKeyValue string
	SortKeyValue string
	JsonValue    []byte
	TextValue    string
	NumericValue float64
	TimeToLive   int64
}

// IsValid returns true if the Record is valid (all required fields are supplied).
func (r *Record) IsValid() bool {
	return r.PartKeyValue != "" && r.SortKeyValue != ""
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

// CountSortKeys returns the total number of sort keys for a specified partition key.
func (rs *RecordSet) CountSortKeys(partKey string) int64 {
	if *rs == nil || (*rs)[partKey] == nil {
		return 0
	}
	return int64(len((*rs)[partKey]))
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

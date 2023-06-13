package main

import (
	"reflect"
	"testing"
	"time"
)

func TestGetRecordsBetween(t *testing.T) {
	layout := "2006-01-02"
	startDate, _ := time.Parse(layout, "2023-01-01")
	endDate, _ := time.Parse(layout, "2023-01-05")

	records := [][]string{
		{"2023-01-01", "data1"},
		{"2023-01-02", "data2"},
		{"2023-01-03", "data3"},
		{"2023-01-04", "data4"},
		{"2023-01-05", "data5"},
	}

	expected := [][]string{
		{"2023-01-01", "data1"},
		{"2023-01-02", "data2"},
		{"2023-01-03", "data3"},
		{"2023-01-04", "data4"},
		{"2023-01-05", "data5"},
	}

	filteredRecords, err := getRecordsBetween(startDate, endDate, records)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !reflect.DeepEqual(filteredRecords, expected) {
		t.Errorf("expected %v, got %v", expected, filteredRecords)
	}
}

func TestGetRecordsBetween_NegativeScenarios(t *testing.T) {
	layout := "2006-01-02"
	startDate, _ := time.Parse(layout, "2023-01-01")
	endDate, _ := time.Parse(layout, "2023-01-05")

	records := [][]string{
		{"2023-01-01", "data1"},
		{"2023-01-02", "data2"},
		{"2023-01-03", "data3"},
		{"2023-01-04", "data4"},
		{"2023-01-05", "data5"},
	}

	// Test with start date after end date
	_, err := getRecordsBetween(endDate, startDate, records)
	if err == nil {
		t.Error("expected an error for start date after end date, but got none")
	}

	// Test with empty records
	_, err = getRecordsBetween(startDate, endDate, [][]string{})
	if err == nil {
		t.Error("expected an error for empty records, but got none")
	}

	// Test with invalid date format in records
	invalidRecords := [][]string{
		{"2023-01-01", "data1"},
		{"2023-01-02", "data2"},
		{"invalid_date", "data3"},
		{"2023-01-04", "data4"},
		{"2023-01-05", "data5"},
	}
	_, err = getRecordsBetween(startDate, endDate, invalidRecords)
	if err == nil {
		t.Error("expected an error for invalid date format in records, but got none")
	}
}

func TestGetRecordsBetween_MissingDates(t *testing.T) {
	layout := "2006-01-02"
	startDate, _ := time.Parse(layout, "2023-01-01")
	endDate, _ := time.Parse(layout, "2023-01-05")

	// Test when the first entry in records is after startDate
	records1 := [][]string{
		{"2023-01-02", "data2"},
		{"2023-01-03", "data3"},
		{"2023-01-04", "data4"},
		{"2023-01-05", "data5"},
	}
	_, err := getRecordsBetween(startDate, endDate, records1)
	if err == nil {
		t.Error("expected an error for first entry after startDate, but got none")
	}

	// Test when the last entry in records is before endDate
	records2 := [][]string{
		{"2023-01-01", "data1"},
		{"2023-01-02", "data2"},
		{"2023-01-03", "data3"},
		{"2023-01-04", "data4"},
	}
	_, err = getRecordsBetween(startDate, endDate, records2)
	if err == nil {
		t.Error("expected an error for last entry before endDate, but got none")
	}

	// Test when a date is missing
	records3 := [][]string{
		{"2023-01-01", "data1"},
		{"2023-01-02", "data2"},
		// Missing 2023-01-03
		{"2023-01-04", "data4"},
		{"2023-01-05", "data5"},
	}
	_, err = getRecordsBetween(startDate, endDate, records3)
	if err == nil {
		t.Error("expected an error for missing date, but got none")
	}
}

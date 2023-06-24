package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/pkg/errors"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

var csvDir = flag.String("d", "csvs", "folder with csv files to merge")
var startDateStr = flag.String("s", "2018-01-01", "start date in YYYY-MM-DD")
var endDateStr = flag.String("e", "2023-01-01", "end date in YYYY-MM-DD")

func main() {
	flag.Parse()

	startDate, err := time.Parse("2006-01-02", *startDateStr)
	if err != nil {
		log.Fatal(err)
	}

	endDate, err := time.Parse("2006-01-02", *endDateStr)
	if err != nil {
		log.Fatal(err)
	}

	files, err := os.ReadDir(*csvDir)
	if err != nil {
		log.Fatal(err)
	}

	totalMissingDates := make(map[string]bool)
	filesPassedFiltering := make([]string, 0)
	allRecs := make([][][]string, 0, len(files)) //csvfile->row->ohlc
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".csv") {
			records, err := getCsvRecords(*csvDir, file)
			if err != nil {
				log.Fatal(err)
			}
			filteredRecords, missingDates, err := getRecordsBetween(startDate, endDate, records)
			if err != nil {
				log.Println(file.Name(), err)
				continue
			}
			for date := range missingDates {
				totalMissingDates[date] = true
			}
			if len(filteredRecords[0]) > 0 {
				allRecs = append(allRecs, filteredRecords)
				filesPassedFiltering = append(filesPassedFiltering, file.Name())
			}
		}
	}

	allRecsFiltered, err := allRecsExceptDates(allRecs, totalMissingDates)
	if err != nil {
		log.Fatal(err)
	}

	mergedFile, err := os.OpenFile(fmt.Sprintf("%s_%s.csv", *startDateStr, *endDateStr), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err = mergedFile.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()
	writer := csv.NewWriter(mergedFile)
	header := []string{"Date"}
	for _, fileName := range filesPassedFiltering {
		symbol := strings.TrimSuffix(fileName, ".csv")
		header = append(header, symbol+"_open", symbol+"_high", symbol+"_low", symbol+"_close", symbol+"_volume")
	}
	err = writer.Write(header)
	if err != nil {
		log.Fatal(err)
	}
	writer.Flush()

	for i := 0; i < len(allRecsFiltered[3]); i++ {
		date := allRecsFiltered[3][i][0]
		dateTime, err := time.Parse("2006-01-02", date)
		if err != nil {
			log.Fatal(err)
		}
		mergedRow := []string{dateTime.UTC().Format("2006-01-02 15:04:05")} //date
		for j := 0; j < len(allRecs); j++ {
			mergedRow = append(mergedRow, allRecsFiltered[j][i][1], allRecsFiltered[j][i][2], allRecsFiltered[j][i][3],
				allRecsFiltered[j][i][4], allRecsFiltered[j][i][6]) //skipping date and adj close
		}
		err = writer.Write(mergedRow)
		if err != nil {
			log.Fatal(err)
		}
		writer.Flush()
	}
}

func getCsvRecords(dir string, file os.DirEntry) ([][]string, error) {
	csvFile, err := os.Open(filepath.Join(dir, file.Name()))
	if err != nil {
		return nil, err
	}
	defer func(csvFile *os.File) {
		err := csvFile.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(csvFile)

	reader := csv.NewReader(csvFile)

	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}
	return records, nil
}

func getRecordsBetween(startDate, endDate time.Time, records [][]string) ([][]string, map[string]bool, error) {
	if endDate.Before(startDate) {
		return nil, nil, errors.New("endDate must be greater than or equal to startDate")
	}

	//layout := "2006-01-02 15:04:05"
	layout := "2006-01-02"
	filteredRecords := make([][]string, 0)

	timeDelta, err := getTimeDelta(records, layout)
	if err != nil {
		return nil, nil, errors.Wrap(err, "get time delta")
	}

	expectedDate := startDate
	recordIndex := 0
	missingDates := make(map[string]bool)

	for !expectedDate.After(endDate) {
		if recordIndex >= len(records) {
			return nil, nil, errors.New("some dates between startDate and endDate are missing")
		}

		record := records[recordIndex]
		if len(record) == 0 {
			continue
		}

		recordDate, err := time.Parse(layout, record[0])
		if err != nil {
			return nil, nil, errors.Wrap(err, "error parsing date")
		}

		if recordDate.Equal(expectedDate) {
			filteredRecords = append(filteredRecords, record)
			recordIndex++
			expectedDate = expectedDate.Add(timeDelta)
		} else if recordDate.Before(expectedDate) {
			recordIndex++
		} else {
			fmt.Println("date is missing", expectedDate.Format(layout))
			missingDates[expectedDate.Format(layout)] = true
			expectedDate = expectedDate.Add(timeDelta)
		}
	}

	return filteredRecords, missingDates, nil
}

func getTimeDelta(records [][]string, layout string) (time.Duration, error) {
	record1 := records[0]
	record2 := records[1]
	recordDate1, err := time.Parse(layout, record1[0])
	if err != nil {
		return 0, errors.Wrap(err, "error parsing date")
	}
	recordDate2, err := time.Parse(layout, record2[0])
	if err != nil {
		return 0, errors.Wrap(err, "error parsing date")
	}
	return recordDate2.Sub(recordDate1), nil
}

func allRecsExceptDates(allRecs [][][]string, missingDates map[string]bool) ([][][]string, error) {
	allRecsFiltered := make([][][]string, 0, len(allRecs)) //csvfile->row->ohlc
	for _, records := range allRecs {
		filtered := getRecordsExceptDates(records, missingDates)
		allRecsFiltered = append(allRecsFiltered, filtered)
	}
	return allRecsFiltered, nil
}

func getRecordsExceptDates(records [][]string, missingDates map[string]bool) [][]string {
	filtered := make([][]string, 0, len(records))
	for _, record := range records {
		if !missingDates[record[0]] {
			filtered = append(filtered, record)
		}
	}
	return filtered
}

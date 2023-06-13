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

	filesPassedFiltering := make([]string, 0)
	allRecs := make([][][]string, 0, len(files)) //csvfile->row->ohlc
	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".csv") {
			records, err := getCsvRecords(*csvDir, file)
			if err != nil {
				log.Fatal(err)
			}
			filteredRecords, err := getRecordsBetween(startDate, endDate, records)
			if err != nil {
				log.Println(file.Name(), err)
				continue
			}
			if len(filteredRecords[0]) > 0 {
				allRecs = append(allRecs, filteredRecords)
				filesPassedFiltering = append(filesPassedFiltering, file.Name())
			}
		}
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
		symbol := strings.TrimSuffix(fileName, "USDT.csv")
		header = append(header, symbol+"_open", symbol+"_high", symbol+"_low", symbol+"_close", symbol+"_volume")
	}
	err = writer.Write(header)
	if err != nil {
		log.Fatal(err)
	}
	writer.Flush()

	for i := 0; i < len(allRecs[0]); i++ {
		mergedRow := []string{allRecs[0][i][0]} //date
		for j := 0; j < len(allRecs); j++ {
			mergedRow = append(mergedRow, allRecs[j][i][1:]...) //kline values for pair, skipping date
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

func getRecordsBetween(startDate, endDate time.Time, records [][]string) ([][]string, error) {
	if endDate.Before(startDate) {
		return nil, errors.New("endDate must be greater than or equal to startDate")
	}

	layout := "2006-01-02 15:04:05"
	filteredRecords := make([][]string, 0)

	timeDelta, err := getTimeDelta(records, layout)
	if err != nil {
		return nil, errors.Wrap(err, "get time delta")
	}

	expectedDate := startDate
	recordIndex := 0

	for !expectedDate.After(endDate) {
		if recordIndex >= len(records) {
			return nil, errors.New("some dates between startDate and endDate are missing")
		}

		record := records[recordIndex]
		if len(record) == 0 {
			continue
		}

		recordDate, err := time.Parse(layout, record[0])
		if err != nil {
			return nil, errors.Wrap(err, "error parsing date")
		}

		if recordDate.Equal(expectedDate) {
			filteredRecords = append(filteredRecords, record)
			recordIndex++
			expectedDate = expectedDate.Add(timeDelta)
		} else if recordDate.Before(expectedDate) {
			recordIndex++
		} else {
			expectedDate = expectedDate.Add(timeDelta)
			fmt.Println("date is missing", recordDate.String())
			//return nil, errors.New("some dates between startDate and endDate are missing")
		}
	}

	return filteredRecords, nil
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

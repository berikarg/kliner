package main

import (
	"encoding/csv"
	"flag"
	"log"
	"os"
	"strings"
)

var csvs = flag.String("c", "", "comma separated list of csv files to merge")

func main() {
	flag.Parse()

	csvList := strings.Split(*csvs, ",")

	allRecs := make([][][]string, 0, len(csvList)) //csvfile->row->ohlc
	for _, fileName := range csvList {
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatal(err)
		}
		csvReader := csv.NewReader(file)
		recs, err := csvReader.ReadAll()
		if err != nil {
			log.Fatal(err)
		}
		allRecs = append(allRecs, recs)
	}

	mergedFile, err := os.OpenFile("all.csv", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
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
	for _, fileName := range csvList {
		symbol := strings.TrimRight(fileName, "USDT.csv")
		header = append(header, symbol+"_VWAP", symbol+"_RSI", symbol+"_open", symbol+"_high", symbol+"_low", symbol+"_close", symbol+"_volume")
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

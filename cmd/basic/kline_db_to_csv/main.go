package main

import (
	"encoding/csv"
	"flag"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"gitlab.com/berik.argimbayev/kliner/internal/config"
	"gitlab.com/berik.argimbayev/kliner/internal/models"
	"gitlab.com/berik.argimbayev/kliner/internal/postgres"
	"log"
	"os"
	"time"
)

var configPath = flag.String("c", "./configs/config.yml", "config file path")

func main() {
	flag.Parse()
	cfg, err := config.New(*configPath)
	if err != nil {
		log.Fatal(err.Error())
	}
	db, err := postgres.OpenPostgres(cfg.Database)
	if err != nil {
		log.Fatal(err.Error())
	}
	start := convertAndRoundToMinute(int64(cfg.StartDate))
	end := convertAndRoundToMinute(int64(cfg.EndDate))
	for _, pair := range cfg.CryptoPairs {
		err := getAndSaveKlines(db, pair, cfg.TimeFrame, start, end, cfg.OutputDir)
		if err != nil {
			log.Println(err)
			continue
		}
	}
}

func convertAndRoundToMinute(unixMilliTimestamp int64) time.Time {
	t := time.Unix(unixMilliTimestamp/1e3, 0)
	return t.Round(time.Minute).UTC()
}

func getAndSaveKlines(db *sqlx.DB, pair string, tf models.TimeFrame, startDate, endDate time.Time, outputDir string) error {
	klines, err := postgres.GetCandles(db, pair, string(tf), startDate, endDate)
	if err != nil {
		return err
	}
	if len(klines) == 0 {
		return errors.Errorf("no candles for %s", pair)
	}

	return fillKlinesInCSV(klines, pair, outputDir, string(tf))
}

func fillKlinesInCSV(klines []models.CandleDB, pair, outputDir, tf string) error {
	csvFile, err := os.OpenFile(outputDir+"/"+pair+"_"+tf+".csv", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	defer func() {
		err = csvFile.Close()
		if err != nil {
			log.Fatal(err.Error())
		}
	}()
	writer := csv.NewWriter(csvFile)

	err = writer.Write([]string{
		"Date",
		pair + "_open",
		pair + "_high",
		pair + "_low",
		pair + "_close",
		pair + "_volume",
	})

	for _, kline := range klines {
		openTime, err := time.Parse(time.RFC3339, kline.OpenTime)
		if err != nil {
			return err
		}
		err = writer.Write([]string{
			openTime.Format("2006-01-02 15:04:05"),
			kline.Open.String(),
			kline.High.String(),
			kline.Low.String(),
			kline.Close.String(),
			kline.Volume.String(),
		})
		writer.Flush()
	}
	return nil
}

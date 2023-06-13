package main

import (
	"encoding/csv"
	"flag"
	"github.com/pkg/errors"
	"gitlab.com/berik.argimbayev/kliner/internal/config"
	"gitlab.com/berik.argimbayev/kliner/internal/models"
	"gitlab.com/berik.argimbayev/kliner/pkg/binance"
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
	for _, pair := range cfg.CryptoPairs {
		klines, err := getKlines(pair, cfg.TimeFrame, cfg.StartDate, cfg.EndDate)
		if err != nil {
			log.Println(err)
			continue
		}
		err = fillKlinesInCSV(pair, klines, cfg.OutputDir)
		if err != nil {
			log.Fatal(err.Error())
		}
	}
}

func getKlines(pair string, tf models.TimeFrame, startDate, endDate int) ([]models.KLine, error) {
	allKlines := make([]models.KLine, 0, 1024)
	for startDate < endDate {
		klines, err := binance.GetKLines(pair, tf, startDate, endDate)
		if err != nil {
			return nil, err
		}
		if len(klines) == 0 {
			return nil, errors.Errorf("no candles for %s", pair)
		}
		allKlines = append(allKlines, klines...)
		startDate = klines[len(klines)-1].CloseTime
	}
	return allKlines, nil
}

func fillKlinesInCSV(pair string, klines []models.KLine, outputDir string) error {
	csvFile, err := os.OpenFile(outputDir+"/"+pair+".csv", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
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
	for _, kline := range klines {
		openTime := time.UnixMilli(int64(kline.OpenTime)).UTC()

		err = writer.Write([]string{
			openTime.Format("2006-01-02 15:04:05"),
			kline.OpenPrice.String(),
			kline.HighPrice.String(),
			kline.LowPrice.String(),
			kline.ClosePrice.String(),
			kline.Volume.String(),
		})
		writer.Flush()
	}
	return nil
}

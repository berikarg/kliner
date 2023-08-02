package main

import (
	"flag"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"gitlab.com/berik.argimbayev/kliner/internal/config"
	"gitlab.com/berik.argimbayev/kliner/internal/models"
	"gitlab.com/berik.argimbayev/kliner/internal/postgres"
	"gitlab.com/berik.argimbayev/kliner/pkg/binance"
	"log"
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
	for _, pair := range cfg.CryptoPairs {
		lastOpenTime, err := postgres.GetLastOpenTime(db, pair+"-F", string(cfg.TimeFrame))
		if err != nil {
			log.Println(err, "use given start time")
			lastOpenTime = time.UnixMilli(int64(cfg.StartDate)).Add(-time.Second) // sub one sec since it is going to be added
		}
		startTime := lastOpenTime.Add(time.Second) // should work for all candles (1m, 1d etc.)
		err = getAndSaveKlines(db, pair, cfg.TimeFrame, int(startTime.UnixMilli()), cfg.EndDate)
		if err != nil {
			log.Println(err)
			continue
		}
	}
}

func getAndSaveKlines(db *sqlx.DB, pair string, tf models.TimeFrame, startDate, endDate int) error {
	for startDate < endDate {
		klines, err := binance.GetFuturesKLines(pair, tf, startDate, endDate)
		if err != nil {
			return err
		}
		if len(klines) == 0 {
			return errors.Errorf("no candles for %s", pair)
		}
		startDate = klines[len(klines)-1].CloseTime

		err = fillKlinesInDB(db, pair, tf, klines)
		if err != nil {
			return err
		}
	}
	return nil
}

func fillKlinesInDB(db *sqlx.DB, pair string, interval models.TimeFrame, klines []models.KLine) error {
	pair += "-F"
	for _, kline := range klines {
		klineDb := models.CandleDB{
			Symbol:   pair,
			Interval: string(interval),
			OpenTime: time.UnixMilli(int64(kline.OpenTime)).UTC().Format("2006-01-02 15:04:05"),
			Open:     kline.OpenPrice,
			High:     kline.HighPrice,
			Low:      kline.LowPrice,
			Close:    kline.ClosePrice,
			Volume:   kline.Volume,
		}
		err := postgres.InsertCandle(db, klineDb)
		if err != nil {
			return errors.Wrapf(err, "save %+v", klineDb)
		}
	}
	return nil
}

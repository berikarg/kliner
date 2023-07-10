package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"gitlab.com/berik.argimbayev/kliner/internal/config"
	"gitlab.com/berik.argimbayev/kliner/internal/models"
	"gitlab.com/berik.argimbayev/kliner/internal/postgres"
	"log"
	"math"
	"os"
	"strings"
	"time"
)

func main() {
	configPath := flag.String("c", "./configs/config.yml", "config file path")
	flag.Parse()
	cfg, err := config.New(*configPath)
	if err != nil {
		log.Fatal(err.Error())
	}
	db, err := postgres.OpenPostgres(cfg.Database)
	if err != nil {
		log.Fatal(err.Error())
	}
	interval, err := time.ParseDuration(string(cfg.TimeFrame))
	if err != nil {
		log.Fatal(err.Error())
	}
	start := convertAndRoundToMinute(int64(cfg.StartDate))
	end := convertAndRoundToMinute(int64(cfg.EndDate))
	var spreadCandles []models.CandleDB
	for start.Before(end) {
		spreadCandle, err := constructKline(db, start, interval, cfg.Spread)
		start = start.Add(interval)
		if err != nil {
			fmt.Println(err)
			continue
		}
		spreadCandles = append(spreadCandles, spreadCandle)
	}
	err = fillKlinesInCSV(cfg.OutputDir, spreadCandles)
	if err != nil {
		log.Fatal(err.Error())
	}
}

func constructKline(db *sqlx.DB, start time.Time, interval time.Duration, spread config.Spread) (models.CandleDB, error) {
	if !isMinuteDataAvailable(db, start, spread.Numerator) || !isMinuteDataAvailable(db, start, spread.Denominator) {
		return models.CandleDB{}, errors.Errorf("%s data unavailable", start.String())
	}
	end := start.Add(interval)

	numCandles, err := postgres.GetCandles(db, spread.Numerator, "1m", start, end)
	if err != nil {
		return models.CandleDB{}, err
	}
	denCandles, err := postgres.GetCandles(db, spread.Denominator, "1m", start, end)
	if err != nil {
		return models.CandleDB{}, err
	}
	spreads, err := calcSpreads(numCandles, denCandles, spread)
	if err != nil {
		return models.CandleDB{}, errors.Wrap(err, "calc high")
	}
	high := decimal.Max(spreads[0], spreads[1:]...)
	low := decimal.Min(spreads[0], spreads[1:]...)
	open := calcSpread(numCandles[0].Open, denCandles[0].Open, spread)
	closePrice := calcSpread(numCandles[len(numCandles)-1].Close, denCandles[len(denCandles)-1].Close, spread)
	return models.CandleDB{
		Symbol:   getSpreadSymbol(spread),
		Interval: interval.String(),
		OpenTime: start.Format("2006-01-02 15:04:05"),
		Open:     open,
		High:     high,
		Low:      low,
		Close:    closePrice,
		Volume:   decimal.Zero,
	}, nil
}

func isMinuteDataAvailable(db *sqlx.DB, timestamp time.Time, pair string) bool {
	_, err := postgres.GetCandle(db, pair, "1m", timestamp)
	return err == nil
}

func convertAndRoundToMinute(unixMilliTimestamp int64) time.Time {
	t := time.Unix(unixMilliTimestamp/1e3, 0)
	return t.Round(time.Minute).UTC()
}

func calcSpreads(numCandles, denCandles []models.CandleDB, spread config.Spread) ([]decimal.Decimal, error) {
	spreads := make([]decimal.Decimal, 0, len(numCandles))
	for i := 0; i < len(numCandles); i++ {
		if numCandles[i].OpenTime != denCandles[i].OpenTime {
			return nil, errors.New("open time mismatch")
		}
		spreads = append(spreads, calcSpread(numCandles[i].Close, denCandles[i].Close, spread))
	}
	return spreads, nil
}

func calcSpread(num, den decimal.Decimal, spread config.Spread) decimal.Decimal {
	numLog := decimal.NewFromFloat(math.Log(num.InexactFloat64()))
	denLog := decimal.NewFromFloat(math.Log(den.InexactFloat64()))
	//log(NUM) - (k * log(DEN) + b)
	return numLog.Sub(spread.K.Mul(denLog).Add(spread.B))
}

func getSpreadSymbol(spread config.Spread) string {
	num := strings.TrimSuffix(spread.Numerator, "USDT")
	den := strings.TrimSuffix(spread.Denominator, "USDT")
	return num + "-" + den
}

func fillKlinesInCSV(outputDir string, klines []models.CandleDB) error {
	csvFile, err := os.OpenFile(outputDir+"/"+klines[0].Symbol+".csv", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
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
		"Open",
		"High",
		"Low",
		"Close",
	})
	if err != nil {
		return err
	}

	for _, kline := range klines {
		err = writer.Write([]string{
			kline.OpenTime,
			kline.Open.String(),
			kline.High.String(),
			kline.Low.String(),
			kline.Close.String(),
		})
		if err != nil {
			return err
		}
		writer.Flush()
	}
	return nil
}

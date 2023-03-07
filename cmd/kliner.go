package main

import (
	"encoding/csv"
	"flag"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
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
		err = fillKlinesInCSV(pair, cfg)
		if err != nil {
			log.Fatal(err.Error())
		}
	}
}

func fillKlinesInCSV(pair string, cfg *config.Config) error {
	csvFile, err := os.OpenFile(pair+".csv", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err.Error())
	}
	defer func() {
		err = csvFile.Close()
		if err != nil {
			log.Fatal(err.Error())
		}
	}()
	writer := csv.NewWriter(csvFile)
	klines, err := getExtendedKlines(pair, cfg.TimeFrame, cfg.StartDate, cfg.EndDate)
	if err != nil {
		log.Fatal(err.Error())
	}
	for _, kline := range klines {
		openTime := time.UnixMilli(int64(kline.OpenTime))

		err = writer.Write([]string{
			openTime.Format("2006-01-02"),
			kline.VWAP.String(),
			kline.HighPrice.String(),
			kline.LowPrice.String(),
			kline.ClosePrice.String(),
			kline.Volume.String(),
		})
		writer.Flush()
	}
	return nil
}

func getExtendedKlines(pair string, tf models.TimeFrame, startDate, endDate int) ([]models.KLineExtended, error) {
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
	kLinesExtended := make([]models.KLineExtended, 0, len(allKlines))
	for _, kline := range allKlines {
		vwap, err := getVWAP(kline, pair, tf)
		if err != nil {
			return nil, err
		}
		kLinesExtended = append(kLinesExtended, models.KLineExtended{
			KLine: kline,
			VWAP:  vwap,
		})
	}
	return kLinesExtended, nil
}

func getVWAP(kline models.KLine, pair string, tf models.TimeFrame) (decimal.Decimal, error) {
	lowerTf, err := getLowerTimeFrame(tf)
	if err != nil {
		return decimal.Decimal{}, errors.Wrap(err, "get lower time frame")
	}
	lowerTfKlines, err := binance.GetKLines(pair, lowerTf, kline.OpenTime, kline.CloseTime)
	if err != nil {
		return decimal.Decimal{}, errors.Wrap(err, "get lower time frame klines")
	}
	typicalPriceTimesVolumeSum, volumeSum := decimal.Zero, decimal.Zero
	for _, kl := range lowerTfKlines {
		//Typical Price = (High + Low + Close) / 3
		typicalPrice := kl.HighPrice.Add(kl.LowPrice).Add(kl.ClosePrice).Div(decimal.NewFromInt(3))
		// ∑ (Typical Price * Volume )
		typicalPriceTimesVolumeSum = typicalPriceTimesVolumeSum.Add(typicalPrice.Mul(kl.Volume))
		volumeSum = volumeSum.Add(kl.Volume)
	}
	//VWAP = ∑ (Typical Price * Volume ) / ∑ Volume
	return typicalPriceTimesVolumeSum.Div(volumeSum), nil
}

func getLowerTimeFrame(interval models.TimeFrame) (models.TimeFrame, error) {
	switch interval {
	case models.Week:
		return models.Day, nil
	case models.Day:
		return models.Hour, nil
	case models.Hour:
		return models.Minute, nil
	default:
		return "", errors.New("invalid timeframe")
	}
}

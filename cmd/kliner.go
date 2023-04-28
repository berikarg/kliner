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

const rsiPeriods = 14

func main() {
	flag.Parse()
	cfg, err := config.New(*configPath)
	if err != nil {
		log.Fatal(err.Error())
	}
	for _, pair := range cfg.CryptoPairs {
		klines, err := getExtendedKlines(pair, cfg.TimeFrame, cfg.StartDate, cfg.EndDate)
		if err != nil {
			log.Println(err)
			continue
		}
		err = fillKlinesInCSV(pair, klines)
		if err != nil {
			log.Fatal(err.Error())
		}
	}
}

func fillKlinesInCSV(pair string, klines []models.KLineExtended) error {
	csvFile, err := os.OpenFile(pair+".csv", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
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
		openTime := time.UnixMilli(int64(kline.OpenTime))

		err = writer.Write([]string{
			openTime.Format("2006-01-02"),
			kline.VWAP.String(),
			kline.RSI.String(),
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
	lastAvgUp, lastAvgDown := decimal.NewFromInt(-1), decimal.Zero
	for i, kline := range allKlines {
		if i < rsiPeriods+1 {
			continue
		}
		vwap, err := getVWAP(kline, pair, tf)
		if err != nil {
			return nil, err
		}

		rsi, err := getRSI(allKlines[i-rsiPeriods+1:i+1], lastAvgUp, lastAvgDown)
		if err != nil {
			return nil, err
		}
		lastAvgUp = rsi[1]
		lastAvgDown = rsi[2]

		kLinesExtended = append(kLinesExtended, models.KLineExtended{
			KLine: kline,
			VWAP:  vwap,
			RSI:   rsi[0],
		})
	}
	return kLinesExtended, nil
}

func getVWAP(kline models.KLine, pair string, tf models.TimeFrame) (decimal.Decimal, error) {
	lowerTf, err := models.GetLowerTimeFrame(tf)
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

func getRSI(klines []models.KLine, lastAvgU, lastAvgD decimal.Decimal) ([3]decimal.Decimal, error) {
	if lastAvgU.IsNegative() {
		return getFirstRSI(klines), nil
	}
	diff := klines[len(klines)-1].ClosePrice.Sub(klines[len(klines)-2].ClosePrice)
	currUp, currDown := decimal.Zero, decimal.Zero
	if diff.IsNegative() {
		currDown = diff.Abs()
	} else {
		currUp = diff
	}
	one := decimal.NewFromInt(1)
	N := decimal.NewFromInt(rsiPeriods)
	//avgU = (lastAvgU*(N-1) + currUp) / N
	avgU := lastAvgU.Mul(N.Sub(one)).Add(currUp).Div(N)
	avgD := lastAvgD.Mul(N.Sub(one)).Add(currDown).Div(N)
	rs := avgU.Div(avgD)
	hundred := decimal.NewFromInt(100)
	//RSI = 100 – 100 / ( 1 + RS )
	return [3]decimal.Decimal{hundred.Sub(hundred.Div(rs.Add(one))), avgU, avgD}, nil
}

// getFirstRSI returns rsi, avg up and avg down for the first iteration
func getFirstRSI(klines []models.KLine) [3]decimal.Decimal {
	ups := make([]decimal.Decimal, 0, len(klines))
	downs := make([]decimal.Decimal, 0, len(klines))
	for i := 1; i < len(klines); i++ {
		diff := klines[i].ClosePrice.Sub(klines[i-1].ClosePrice)
		if diff.IsNegative() {
			ups = append(ups, decimal.Zero)
			downs = append(downs, diff.Abs())
		} else {
			ups = append(ups, diff)
			downs = append(downs, decimal.Zero)
		}
	}
	//AvgU = sum of all up moves (U) in the last N bars divided by N
	avgU := decimal.Avg(ups[0], ups...)
	avgD := decimal.Avg(downs[0], downs...)
	rs := avgU.Div(avgD)
	hundred := decimal.NewFromInt(100)
	one := decimal.NewFromInt(1)
	//RSI = 100 – 100 / ( 1 + RS )
	return [3]decimal.Decimal{hundred.Sub(hundred.Div(rs.Add(one))), avgU, avgD}
}

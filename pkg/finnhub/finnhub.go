package finnhub

import (
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"github.com/shopspring/decimal"
	"gitlab.com/berik.argimbayev/kliner/internal/models"
	"io"
	"net/http"
	"net/url"
	"time"
)

type Candles struct {
	ClosePrices []float64 `json:"c"`
	HighPrices  []float64 `json:"h"`
	LowPrices   []float64 `json:"l"`
	OpenPrices  []float64 `json:"o"`
	Status      string    `json:"s"`
	Timestamps  []int     `json:"t"`
	Volumes     []float64 `json:"v"`
}

func GetKLines(symbol string, interval models.TimeFrame, startTime, endTime int) ([]models.KLine, error) {
	values := url.Values{}
	values.Set("symbol", symbol)
	resolution, intervalInSeconds, err := getResolutionAndSecondsEquivalent(interval)
	if err != nil {
		return nil, err
	}
	values.Set("resolution", resolution)
	values.Set("from", fmt.Sprintf("%d", startTime))
	values.Set("to", fmt.Sprintf("%d", endTime))
	values.Set("token", "cg31nvhr01qtum0h83cgcg31nvhr01qtum0h83d0")
	path := (&url.URL{
		Scheme:   "https",
		Host:     "finnhub.io",
		Path:     "api/v1/stock/candle",
		RawQuery: values.Encode(),
	}).String()

	var resp Candles
	err = sendHTTPRequest(http.MethodGet, path, nil, nil, &resp)
	if err != nil {
		return nil, err
	}
	if resp.Status != "ok" {
		return nil, errors.New(resp.Status)
	}
	klines := make([]models.KLine, 0, len(resp.OpenPrices))
	for i := 0; i < len(resp.OpenPrices); i++ {
		klines = append(klines, models.KLine{
			OpenTime:   resp.Timestamps[i],
			OpenPrice:  decimal.NewFromFloat(resp.OpenPrices[i]),
			HighPrice:  decimal.NewFromFloat(resp.HighPrices[i]),
			LowPrice:   decimal.NewFromFloat(resp.LowPrices[i]),
			ClosePrice: decimal.NewFromFloat(resp.ClosePrices[i]),
			Volume:     decimal.NewFromFloat(resp.Volumes[i]),
			CloseTime:  resp.Timestamps[i] + intervalInSeconds - 1,
		})
	}
	return klines, nil
}

// getResolutionAndSecondsEquivalent returns resolution needed for finnhub and number of seconds
func getResolutionAndSecondsEquivalent(interval models.TimeFrame) (string, int, error) {
	switch interval {
	case models.Week:
		return "W", 7 * 24 * 60 * 60, nil
	case models.Day:
		return "D", 24 * 60 * 60, nil
	case models.Hour:
		return "60", 60 * 60, nil
	case models.Minute:
		return "1", 60, nil
	default:
		return "", 0, errors.New("invalid interval")
	}
}

func sendHTTPRequest(method, path string, headers map[string]string, body io.Reader, result interface{}) error {
	req, err := http.NewRequest(method, path, body)
	if err != nil {
		return errors.Wrap(err, "create new http request")
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}
	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	resp, err := client.Do(req)
	if err != nil {
		return errors.Wrap(err, "send http request")
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrap(err, "read response body")
	}
	if resp.StatusCode != 200 {
		return errors.New(string(respBody))
	}

	err = jsoniter.Unmarshal(respBody, &result)
	if err != nil {
		return errors.Wrap(err, "parse response bytes to struct")
	}
	return nil
}

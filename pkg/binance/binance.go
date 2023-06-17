package binance

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

func GetKLines(pair string, interval models.TimeFrame, startTime, endTime int) ([]models.KLine, error) {
	values := url.Values{}
	values.Set("symbol", pair)
	values.Set("interval", string(interval))
	values.Set("startTime", fmt.Sprintf("%d", startTime))
	if endTime != 0 {
		values.Set("endTime", fmt.Sprintf("%d", endTime))
	}
	values.Set("limit", "1000")

	path := (&url.URL{
		Scheme:   "https",
		Host:     "api.binance.com",
		Path:     "api/v3/klines",
		RawQuery: values.Encode(),
	}).String()

	var resp [][]interface{}
	err := sendHTTPRequest(http.MethodGet, path, nil, nil, &resp)
	if err != nil {
		return nil, errors.Wrap(err, "send public http request")
	}
	klines := make([]models.KLine, 0, len(resp))
	for _, rec := range resp {
		openPrice, _ := decimal.NewFromString(rec[1].(string))
		highPrice, _ := decimal.NewFromString(rec[2].(string))
		lowPrice, _ := decimal.NewFromString(rec[3].(string))
		closePrice, _ := decimal.NewFromString(rec[4].(string))
		volume, _ := decimal.NewFromString(rec[7].(string))
		klines = append(klines, models.KLine{
			OpenTime:   int(rec[0].(float64)),
			OpenPrice:  openPrice,
			HighPrice:  highPrice,
			LowPrice:   lowPrice,
			ClosePrice: closePrice,
			Volume:     volume,
			CloseTime:  int(rec[6].(float64)),
		})
	}
	return klines, nil
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
		Timeout: 60 * time.Second,
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

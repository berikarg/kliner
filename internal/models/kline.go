package models

import (
	"github.com/shopspring/decimal"
)

type KLine struct {
	OpenTime   int
	OpenPrice  decimal.Decimal
	HighPrice  decimal.Decimal
	LowPrice   decimal.Decimal
	ClosePrice decimal.Decimal
	Volume     decimal.Decimal
	CloseTime  int
}

type KLineExtended struct {
	KLine
	VWAP decimal.Decimal //Volume Weighted Average Price
	RSI  decimal.Decimal
}

type CandleDB struct {
	Symbol   string          `db:"symbol"`
	Interval string          `db:"interval"`
	OpenTime string          `db:"open_time"`
	Open     decimal.Decimal `db:"open"`
	High     decimal.Decimal `db:"high"`
	Low      decimal.Decimal `db:"low"`
	Close    decimal.Decimal `db:"close"`
	Volume   decimal.Decimal `db:"volume"`
}

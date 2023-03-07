package models

import "github.com/shopspring/decimal"

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

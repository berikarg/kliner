package models

type TimeFrame string

const (
	Week   TimeFrame = "1w"
	Day    TimeFrame = "1d"
	Hour   TimeFrame = "1h"
	Minute TimeFrame = "1m"
)

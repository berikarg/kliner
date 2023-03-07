package models

import "github.com/pkg/errors"

type TimeFrame string

const (
	Week   TimeFrame = "1w"
	Day    TimeFrame = "1d"
	Hour   TimeFrame = "1h"
	Minute TimeFrame = "1m"
)

func GetLowerTimeFrame(interval TimeFrame) (TimeFrame, error) {
	switch interval {
	case Week:
		return Day, nil
	case Day:
		return Hour, nil
	case Hour:
		return Minute, nil
	default:
		return "", errors.New("invalid timeframe")
	}
}

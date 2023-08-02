package postgres

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"gitlab.com/berik.argimbayev/kliner/internal/config"
	"gitlab.com/berik.argimbayev/kliner/internal/models"
	"strings"
	"time"
)

// OpenPostgres opens connection to postgresql db.
//
// Warning: you need to close db.
func OpenPostgres(cfg config.Database) (*sqlx.DB, error) {
	addr := strings.Split(cfg.Addr, ":")
	if len(addr) != 2 {
		return nil, errors.New("invalid db address")
	}
	host, port := addr[0], addr[1]
	db, err := sqlx.Open("postgres", fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=disable",
		host, port, cfg.Name, cfg.User, cfg.Password))
	if err != nil {
		return nil, errors.Wrap(err, "connect to db")
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(20)
	db.SetConnMaxIdleTime(5 * time.Minute)
	if err = db.Ping(); err != nil {
		return nil, errors.Wrap(err, "ping db")
	}
	return db, nil
}

func InsertCandle(db *sqlx.DB, candle models.CandleDB) error {
	query := `INSERT INTO candles (symbol, interval, open_time, open, high, low, close, volume)
		VALUES (:symbol, :interval, :open_time, :open, :high, :low, :close, :volume)`
	_, err := db.NamedExec(query, candle)
	return err
}

func GetCandle(db *sqlx.DB, symbol, interval string, openTime time.Time) (models.CandleDB, error) {
	query := `SELECT * FROM candles WHERE symbol = $1 AND interval = $2 AND open_time = $3;`
	candle := models.CandleDB{}
	err := db.Get(&candle, query, symbol, interval, openTime)
	if err != nil {
		return models.CandleDB{}, err
	}
	return candle, nil
}

func GetCandles(db *sqlx.DB, symbol, interval string, startTime, endTime time.Time) ([]models.CandleDB, error) {
	query := `SELECT * FROM candles
         WHERE symbol = $1
         AND interval = $2
         AND open_time >= $3
         AND open_time < $4
         ORDER BY open_time;`
	var candles []models.CandleDB
	err := db.Select(&candles, query, symbol, interval, startTime, endTime)
	if err != nil {
		return nil, err
	}
	return candles, nil
}

func GetLastOpenTime(db *sqlx.DB, symbol, interval string) (time.Time, error) {
	query := `SELECT open_time FROM candles
				 WHERE symbol = $1
				 AND interval = $2
				 ORDER BY open_time DESC
				 LIMIT 1;`
	var openTime time.Time
	err := db.Get(&openTime, query, symbol, interval)
	if err != nil {
		return time.Time{}, err
	}
	return openTime, nil
}

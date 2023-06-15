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

-- +goose Up
-- +goose StatementBegin
CREATE TABLE candles (
     symbol CHAR(50) NOT NULL,
     interval CHAR(10) NOT NULL,
     open_time TIMESTAMP(0) NOT NULL,
     open DECIMAL NOT NULL,
     high DECIMAL NOT NULL,
     low DECIMAL NOT NULL,
     close DECIMAL NOT NULL,
     volume DECIMAL NOT NULL,
     PRIMARY KEY (symbol, interval, open_time)
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE candles;
-- +goose StatementEnd

package config

import (
	"github.com/pkg/errors"
	"gitlab.com/berik.argimbayev/kliner/internal/models"
	"gopkg.in/yaml.v3"
	"os"
)

type Config struct {
	CryptoPairs []string         `yaml:"crypto_pairs"`
	StartDate   int              `yaml:"start_date"`
	EndDate     int              `yaml:"end_date"`
	TimeFrame   models.TimeFrame `yaml:"time_frame"`
	OutputDir   string           `yaml:"output_dir"`
}

func New(filepath string) (*Config, error) {
	fileBytes, err := os.ReadFile(filepath)
	if err != nil {
		return nil, errors.Wrapf(err, "read file %s", filepath)
	}
	var cfg Config
	if err = yaml.Unmarshal(fileBytes, &cfg); err != nil {
		return nil, errors.Wrap(err, "unmarshal yaml")
	}
	return &cfg, nil
}

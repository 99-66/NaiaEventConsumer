package config

import (
	"errors"
	"github.com/caarlos0/env"
)

type NounsApi struct {
	Url string `env:"NOUNS_API"`
}

func InitNounsApi() (*NounsApi, error) {
	nounsApi := NounsApi{}
	if err := env.Parse(&nounsApi); err != nil {
		return nil, errors.New("cloud not load nouns api environment variables")
	}

	return &nounsApi, nil
}
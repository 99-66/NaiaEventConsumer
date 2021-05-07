package config

import (
	"errors"
	"github.com/caarlos0/env"
	"github.com/cenkalti/backoff/v4"
	elastic "github.com/elastic/go-elasticsearch/v7"
	"time"
)

type ElasticSearch struct {
	Host     []string `env:"ELS_HOST" envSeparator:","`
	User     string   `env:"ELS_USER"`
	Password string   `env:"ELS_PASSWORD"`
}

func newElasticSearch(host []string, username, password string) (*elastic.Client, error) {
	retryBackoff := backoff.NewExponentialBackOff()
	var cfg elastic.Config

	if username != "" && password != "" {
		cfg = elastic.Config{
			Addresses:     host,
			Username:      username,
			Password:      password,
			RetryOnStatus: []int{502, 503, 504, 429},
			RetryBackoff: func(i int) time.Duration {
				if i == 1 {
					retryBackoff.Reset()
				}
				return retryBackoff.NextBackOff()
			},
			MaxRetries: 5,
		}
	} else {
		cfg = elastic.Config{
			Addresses:     host,
			RetryOnStatus: []int{502, 503, 504, 429},
			RetryBackoff: func(i int) time.Duration {
				if i == 1 {
					retryBackoff.Reset()
				}
				return retryBackoff.NextBackOff()
			},
			MaxRetries: 5,
		}
	}
	es, err := elastic.NewClient(cfg)
	if err != nil {
		return nil, err
	}

	return es, nil
}

func InitElasticSearch() (*elastic.Client, error) {
	elsConfig := ElasticSearch{}
	if err := env.Parse(&elsConfig); err != nil {
		return nil, errors.New("cloud not load elasticsearch environment variables")
	}

	return newElasticSearch(elsConfig.Host, elsConfig.User, elsConfig.Password)
}

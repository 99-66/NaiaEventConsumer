package config

import (
	"errors"
	"github.com/caarlos0/env"
	elastic "github.com/elastic/go-elasticsearch/v7"
)

type ElasticSearch struct {
	Host []string `env:"ELS_HOST" envSeparator:","`
	User string `env:"ELS_USER"`
	Password string `env:"ELS_PASSWORD"`
}

func newElasticSearch(host []string, username, password string) (*elastic.Client, error){
	var cfg elastic.Config
	if username != "" && password != "" {
		cfg = elastic.Config{
			Addresses: host,
			Username: username,
			Password: password,
		}
	} else {
		cfg = elastic.Config{
			Addresses: host,
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


package config

import (
	"errors"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/caarlos0/env"
	"time"
)

type Kafka struct {
	Broker []string `env:"BROKER" envSeparator:","`
	Topics []string `env:"TOPICS" envSeparator:","`
	ConsumerGroup string `env:"ConsumerGroup"`
}

func newKafka(kConfig Kafka) (*cluster.Consumer, error){
	conf := cluster.NewConfig()
	conf.Consumer.Return.Errors = true
	conf.Group.Return.Notifications = true
	conf.Consumer.Offsets.CommitInterval = time.Second

	consumer, err := cluster.NewConsumer(kConfig.Broker, kConfig.ConsumerGroup, kConfig.Topics, conf)
	if err != nil {
		return nil, err
	}

	return consumer, nil

}

// InitKafka Kafka 초기 정보를 반환한다
func InitKafka() (*cluster.Consumer, error) {
	kafkaConfig := Kafka{}
	if err := env.Parse(&kafkaConfig); err != nil {
		return nil, errors.New("cloud not load kafka environment variables")
	}

	return newKafka(kafkaConfig)
}

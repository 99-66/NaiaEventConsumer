package main

import (
	"encoding/json"
	"fmt"
	"github.com/99-66/NaiaEventConsumer/config"
	"github.com/99-66/NaiaEventConsumer/controllers"
	"github.com/99-66/NaiaEventConsumer/models"
	"log"
	"os"
	"os/signal"
)

func main() {
	// Nouns(명사추출기) API 정보를 초기화한다
	nounsConfig, err := config.InitNounsApi()
	if err != nil {
		panic(err)
	}

	// ElasticSearch Client 정보를 초기화한다
	es, err := config.InitElasticSearch()
	if err != nil {
		panic(err)
	}

	// Kafka 정보를 초기화한다
	consumer, err := config.InitKafka()
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	// OS 시그널로 종료 트리거를 설정한다
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// 컨슈머 에러를 로깅한다
	go func() {
		for err := range consumer.Errors() {
			log.Printf("Error: %s\n", err.Error())
		}
	}()

	// 컨슈머 알림을 로깅한다
	go func() {
		for noti := range consumer.Notifications() {
			log.Printf("Rebalanced: %+v\n", noti)
		}
	}()

	// 컨슈머로 메시지(이벤트)를 가져온다
	for {
		select {
			case msg, ok := <- consumer.Messages():
				if ok {
					// 컨슈머에서 읽어온 메시지를 언마샬링한다
					var eventVal models.Event
					err = json.Unmarshal(msg.Value, &eventVal)
					if err != nil {
						log.Printf("event failed unmarshling, %s\n", msg.Value)
					}

					// 메시지 처리
					go func() {
						// 1. 텍스트를 명사로 추출한다
						words, err := controllers.NounsExtract(eventVal.Text, nounsConfig.Url)
						if err != nil {
							fmt.Printf("extract failed to nouns. %v\t%s\n", eventVal, err)
						}
						// 2. 추출한 명사별로 ElasticSearch에 저장한다
						for _, word := range words {
							err = controllers.InsertElasticSearch(es, word, eventVal)
							if err != nil {
								log.Printf("an error occurred while elasticsearch insert. %s\n", err)
							}
						}
					}()
					// 메시지를 처리한 것으로 마킹한다
					consumer.MarkOffset(msg, "")
				}
			case <-signals:
				// 종료 시그널(인터럽트)가 들어오면 종료한다
				return
		}
	}
}
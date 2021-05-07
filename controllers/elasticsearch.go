package controllers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/99-66/NaiaEventConsumer/models"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"log"
	"os"
	"time"
)

type WINWord struct {
	Origin             string `json:"origin"`
	Tag                string `json:"tag"`
	CreatedAt          string `json:"createdAt"`
	CreatedAtTimestamp int64  `json:"createdAtTimestamp"`
	Word               string `json:"word"`
}

type WINWords struct {
	Origin             string   `json:"origin"`
	Tag                string   `json:"tag"`
	CreatedAt          string   `json:"createdAt"`
	CreatedAtTimestamp int64    `json:"createdAtTimestamp"`
	Words              []string `json:"words"`
	Text               string   `json:"text"`
}

func InsertElasticsearch(es *elasticsearch.Client, words []string, event models.Event) error {
	err := insertToTextIndex(es, words, &event)
	if err != nil {
		return fmt.Errorf("TEXT: %s\n", err)
	}

	err = insertToWINIndex(es, words, &event)
	if err != nil {
		return fmt.Errorf("WIN: %s\n", err)
	}

	return nil
}

func insertToWINIndex(es *elasticsearch.Client, words []string, event *models.Event) error {
	index := os.Getenv("ELS_WIN_INDEX")

	// 메시지 생성일로 타임스탬프를 만든다
	createdAtTimestamp, err := time.Parse(time.RFC3339, event.CreatedAt)
	if err != nil {
		panic(err)
	}

	// Words 단어 목록들을 bulk index로 저장한다
	// BulkIndexer 생성
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:  index,
		Client: es,
	})
	if err != nil {
		return err
	}

	for _, word := range words {
		// 저장할 데이터 준비: encode WINWord to JSON
		winData, err := json.Marshal(WINWord{
			Tag:                event.Tag,
			Origin:             event.Origin,
			CreatedAt:          event.CreatedAt,
			CreatedAtTimestamp: createdAtTimestamp.Unix(),
			Word:               word,
		})
		if err != nil {
			log.Printf("Cannot encode win word event %+v\n", event)
		}

		// BulkIndexer에 데이터 추가
		err = bi.Add(
			context.Background(),
			esutil.BulkIndexerItem{
				Action: "index",
				Body:   bytes.NewReader(winData),
				OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
					if err != nil {
						log.Printf("ERROR: %s", err)
					} else {
						log.Printf("ERROR: %s: %s", res.Error.Type, res.Error.Reason)
					}
				},
			},
		)
		if err != nil {
			return err
		}
	}

	if err := bi.Close(context.Background()); err != nil {
		return err
	}

	return nil
}

func insertToTextIndex(es *elasticsearch.Client, words []string, event *models.Event) error {
	textIndex := os.Getenv("ELS_TEXT_INDEX")

	// 메시지 생성일로 타임스탬프를 만든다
	createdAtTimestamp, err := time.Parse(time.RFC3339, event.CreatedAt)
	if err != nil {
		panic(err)
	}

	// Words 단어 목록을 index로 저장한다
	// ElasticSearch에 저장할 데이터를 생성한다
	textData, err := json.Marshal(WINWords{
		Tag:                event.Tag,
		Origin:             event.Origin,
		CreatedAt:          event.CreatedAt,
		CreatedAtTimestamp: createdAtTimestamp.Unix(),
		Words:              words,
		Text:               event.Text,
	})

	req := esapi.IndexRequest{
		Index:   textIndex,
		Body:    bytes.NewBuffer(textData),
		Refresh: "true",
	}

	// ElasticSearch에 데이터를 저장한다
	resp, err := req.Do(context.Background(), es)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.IsError() {
		return errors.New(resp.String())
	}

	return nil
}

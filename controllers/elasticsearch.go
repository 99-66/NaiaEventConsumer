package controllers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/99-66/NaiaEventConsumer/models"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"os"
	"time"
)

type WINWord struct {
	Origin string `json:"origin"`
	Tag string `json:"tag"`
	CreatedAt string `json:"createdAt"`
	CreatedAtTimestamp int64 `json:"createdAtTimestamp"`
	Word string `json:"word"`
}

// InsertElasticSearch ElasticSearch로 이벤트를 저장한다
func InsertElasticSearch(es *elasticsearch.Client, word string, event models.Event) error {
	// ElasticSearch Index
	index := os.Getenv("ELS_INDEX")

	// 메시지 생성일로 타임스탬프를 만든다
	createdAtTimestamp, err := time.Parse(time.RFC3339, event.CreatedAt)
	if err != nil {
		panic(err)
	}

	// ElasticSearch에 저장할 데이터를 생성한다
	insertJson, err := json.Marshal(WINWord{
		Tag: event.Tag,
		Origin: event.Origin,
		CreatedAt: event.CreatedAt,
		CreatedAtTimestamp: createdAtTimestamp.Unix(),
		Word: word,
	})


	req := esapi.IndexRequest{
		Index: index,
		Body: bytes.NewBuffer(insertJson),
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
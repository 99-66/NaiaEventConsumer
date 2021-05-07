package controllers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/99-66/NaiaEventConsumer/models"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"github.com/elastic/go-elasticsearch/v7/esutil"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
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

func InsertElasticsearch(es *elasticsearch.Client, words []string, event models.Event) {
	var wg sync.WaitGroup
	wg.Add(2)

	go insertToTextIndex(&wg, es, words, &event)
	go insertToWINIndex(&wg, es, words, &event)

	wg.Wait()
}

func insertToWINIndex(wg *sync.WaitGroup, es *elasticsearch.Client, words []string, event *models.Event) {
	fn := functionName()
	defer wg.Done()

	index := os.Getenv("ELS_WIN_INDEX")

	// 메시지 생성일로 타임스탬프를 만든다
	createdAtTimestamp, err := time.Parse(time.RFC3339, event.CreatedAt)
	if err != nil {
		log.Printf("ERROR %s %s\n", fn, err)
		runtime.Goexit()
	}

	// Words 단어 목록들을 bulk index로 저장한다
	// BulkIndexer 생성
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:  index,
		Client: es,
	})
	if err != nil {
		log.Printf("ERROR %s %s\n", fn, err)
		runtime.Goexit()
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
			log.Printf("ERRPR %s Cannot encode win word event %+v\n", fn, event)
			runtime.Goexit()
		}

		// BulkIndexer에 데이터 추가
		err = bi.Add(
			context.Background(),
			esutil.BulkIndexerItem{
				Action: "index",
				Body:   bytes.NewReader(winData),
				OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
					if err != nil {
						log.Printf("ERROR %s %s\n", fn, err)
					} else {
						log.Fatalf("ERROR %s %s %s\n", fn, res.Error.Type, res.Error.Reason)
					}
				},
			},
		)
		if err != nil {
			log.Printf("ERROR %s %s\n", fn, err)
			runtime.Goexit()
		}
	}

	if err := bi.Close(context.Background()); err != nil {
		log.Printf("ERROR: %s\n", err)
		runtime.Goexit()
	}
}

func insertToTextIndex(wg *sync.WaitGroup, es *elasticsearch.Client, words []string, event *models.Event) {
	fn := functionName()
	defer wg.Done()

	textIndex := os.Getenv("ELS_TEXT_INDEX")

	// 메시지 생성일로 타임스탬프를 만든다
	createdAtTimestamp, err := time.Parse(time.RFC3339, event.CreatedAt)
	if err != nil {
		log.Printf("ERROR %s %s\n", fn, err)
		runtime.Goexit()
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
		log.Printf("ERROR %s %s\n", fn, err)
		runtime.Goexit()
	}
	defer resp.Body.Close()

	if resp.IsError() {
		log.Printf("ERRPR %s %s\n", fn, errors.New(resp.String()))
	}
}

func functionName() (fnName string) {
	pc, _, _, _ := runtime.Caller(1)

	fn := runtime.FuncForPC(pc)

	if fn == nil {
		fnName = "?()"
	} else {
		dotName := filepath.Ext(fn.Name())
		fnName = strings.TrimLeft(dotName, ".") + "()"
	}

	return fnName
}

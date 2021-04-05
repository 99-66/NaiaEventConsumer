package controllers

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
)

type Nouns struct {
	Idxs []int `json:"idxs"`
	Txts []string `json:"txts"`
	Nouns [][]string `json:"nouns"`
}


// Extract API로 텍스트에서 명사 추출을 요청한다
func (n *Nouns) Extract(url string) ([]string, error){
	// Request Body를 마샬링한다
	nbJson, err := json.Marshal(n)
	if err != nil {
		return nil, errors.New("failed request marshaling")
	}

	// API로 명사 추출을 요청한다
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(nbJson))
	if err != nil {
		return nil, errors.New("request failed")
	}
	defer resp.Body.Close()

	// API 응답을 언마샬링한다
	var nResp Nouns
	err = json.NewDecoder(resp.Body).Decode(&nResp)
	if err != nil {
		return nil, errors.New(err.Error())
	}

	// API 응답에서 추출한 명사 목록만 정리하여 반환한다
	var words []string
	for _, nouns := range nResp.Nouns {
		for _, noun := range nouns {
			words = append(words, noun)
		}
	}

	return words, nil
}


// NounsExtract 단일 텍스트 문장에서 명사를 추출한다(텍스트 단일로 요청)
func NounsExtract(text string, url string) ([]string, error) {
	// 텍스트를 API로 요청하기 위해 Request Body를 만든다
	nouns := Nouns{
		Idxs: []int{0},
		Txts: []string{text},
	}

	return nouns.Extract(url)
}

// NounsExtract 복수 텍스트 문장에서 명사를 추출한다
func NounsExtracts(text []string, url string) ([]string, error) {
	// 텍스트들을 API로 요청하기 위해 Request Body를 만든다
	var nouns Nouns
	for i := range text {
		nouns.Idxs = append(nouns.Idxs, 0)
		nouns.Txts = append(nouns.Txts, text[i])
	}

	return nouns.Extract(url)
}
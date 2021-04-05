package models

type Event struct {
	Text string `json:"text" binding:"required"`
	CreatedAt string `json:"created_at" binding:"required"`
	Origin string `json:"origin" binding:"required"`
	Tag string `json:"tag" binding:"required"`
}

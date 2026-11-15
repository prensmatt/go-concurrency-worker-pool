package main

import(
	"encoding/json"
	"time"
)

type Job struct{
	ID          int64                `json:"id"`
	JobID       string               `json:"job_id"`
	Type        string               `json:"type"`
	Payload     json.RawMessage      `json:"payload"`
	Status      string               `json:"status"`
	CreatedAt   time.Time            `json:"created_at"`
}

type CreateJobRequest struct{
	JobID   string						`json:"job_id"`					
	Type    string						`json:"type"`
	Payload json.RawMessage		`json:"payload"`
}
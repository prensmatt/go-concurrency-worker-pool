package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/segmentio/kafka-go"
)

func CreateJob(w http.ResponseWriter, r *http.Request, db *sql.DB, writer *kafka.Writer) {

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req CreateJobRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if strings.TrimSpace(req.JobID) == "" {
		http.Error(w, "Job ID cannot be blank", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.Type) == "" {
		http.Error(w, "Type cannot be blank", http.StatusBadRequest)
		return
	}
	if len(req.Payload) == 0 {
		http.Error(w, "Payload cannot be blank", http.StatusBadRequest)
		return
	}

	if !json.Valid(req.Payload) {
		http.Error(w, "Invalid payload JSON", http.StatusBadRequest)
		return
	}

	query := `
		INSERT INTO jobs (job_id, type, payload)
		VALUES ($1, $2, $3)
		RETURNING id, status, created_at
	`

	var job Job

	job.JobID = strings.TrimSpace(req.JobID)
	job.Type = strings.TrimSpace(req.Type)
	job.Payload = req.Payload

	err = db.QueryRowContext(
		r.Context(),
		query,
		job.JobID,
		job.Type,
		job.Payload,
	).Scan(
		&job.ID,
		&job.Status,
		&job.CreatedAt,
	)

	if err != nil {
		http.Error(w, "Failed to create Job", http.StatusInternalServerError)
		return
	}

	err = PublishJob(r.Context(), writer, JobMessage{
		JobID:   job.JobID,
		Type:    job.Type,
		Payload: job.Payload,
	})

	if err != nil {
		http.Error(w, "Failed to publish job", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	if err := json.NewEncoder(w).Encode(job); err != nil {
		return
	}
}

func GetJobs(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	query := `SELECT id, job_id, type, payload, status, created_at FROM jobs`

	rows, err := db.QueryContext(r.Context(), query)
	if err != nil {
		http.Error(w, "Failed to fetch jobs", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	jobs := make([]Job, 0)

	for rows.Next() {
		var job Job
		err := rows.Scan(&job.ID, &job.JobID, &job.Type, &job.Payload, &job.Status, &job.CreatedAt)
		if err != nil {
			http.Error(w, "Could not scan jobs", http.StatusInternalServerError)
			return
		}
		jobs = append(jobs, job)
	}
	if rows.Err() != nil {
		http.Error(w, "Could not scan jobs", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(jobs); err != nil {
		return
	}
}

func GetJobByID(w http.ResponseWriter, r *http.Request, db *sql.DB) {

	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, "Could not convert string to integer", http.StatusBadRequest)
		return
	}
	if id <= 0 {
		http.Error(w, "Id must be greater than 0", http.StatusBadRequest)
		return
	}

	query := `SELECT id, job_id, type, payload, status, created_at 
	FROM jobs WHERE id = $1`

	var job Job

	err = db.QueryRowContext(r.Context(), query, id).Scan(&job.ID, &job.JobID, &job.Type, &job.Payload, &job.Status, &job.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "Could not fetch job", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	if err := json.NewEncoder(w).Encode(job); err != nil {
		return
	}
}

func UpdateJob(w http.ResponseWriter, r *http.Request, db *sql.DB) {
	if r.Method != http.MethodPut {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, "Could not convert to integer", http.StatusBadRequest)
		return
	}
	if id <= 0 {
		http.Error(w, "ID must be greater than 0", http.StatusBadRequest)
		return
	}
	var req UpdateJobRequest
	err = json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, "Invalid JSON value", http.StatusBadRequest)
		return
	}
	if req.Status == "" {
		http.Error(w, "Invalid Status value", http.StatusBadRequest)
		return
	}
	switch req.Status {
	case "pending":
	case "processing":
	case "completed":
	case "failed":
	default:
		http.Error(w, "Invalid Status value", http.StatusBadRequest)
		return
	}

	query := `UPDATE jobs SET status = $1 WHERE id = $2`
	result, err := db.ExecContext(r.Context(), query, req.Status, id)

	if err != nil {
		http.Error(w, "could not update job", http.StatusInternalServerError)
		return
	}
	rows, err := result.RowsAffected()
	if err != nil {
		http.Error(w, "Could not get affected rows", http.StatusInternalServerError)
		return
	}
	if rows == 0 {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func DeleteJob(w http.ResponseWriter, r *http.Request, db *sql.DB) {

	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	idStr := r.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)

	if err != nil {
		http.Error(w, "Invalid ID", http.StatusBadRequest)
		return
	}
	if id <= 0 {
		http.Error(w, "id must be greater than 0", http.StatusBadRequest)
		return
	}

	query := `DELETE FROM jobs WHERE id=$1`
	result, err := db.ExecContext(r.Context(), query, id)
	if err != nil {
		http.Error(w, "could not delete job", http.StatusInternalServerError)
		return
	}
	rows, err := result.RowsAffected()
	if err != nil {
		http.Error(w, "could not get affected rows", http.StatusInternalServerError)
		return
	}
	if rows == 0 {
		http.Error(w, "job not found", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

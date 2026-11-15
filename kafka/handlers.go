package main

import(
	"net/http"
	"database/sql"
	"encoding/json"
	"strings"
)

func CreateJob(w http.ResponseWriter,r *http.Request, db *sql.DB){
	
	if r.Method != http.MethodPost{
		http.Error(w,"Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	
	var req CreateJobRequest

	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
			http.Error(w, "Invalid JSON", http.StatusBadRequest)
			return
	}

	if strings.TrimSpace(req.JobID) == ""{
		http.Error(w, "Job ID cannot be blank", http.StatusBadRequest)
		return
	}
	if strings.TrimSpace(req.Type) == ""{
		http.Error(w, "Type cannot be blank", http.StatusBadRequest)
		return
	}
	if len(req.Payload) == 0{
		http.Error(w, "Payload cannot be blank", http.StatusBadRequest)
		return
	}

	if !json.Valid(req.Payload){
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
		req.JobID,
		req.Type,
		req.Payload,
	).Scan(
		&job.ID,
		&job.Status,
		&job.CreatedAt,
	)

	if err != nil{
		http.Error(w, "Failed to create Job", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type","application/json")
	w.WriteHeader(http.StatusCreated)

	if err := json.NewEncoder(w).Encode(job); err != nil{
		return
	}
}
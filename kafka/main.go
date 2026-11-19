package main

import (
	"context"
	"log"
	"net/http"

	"github.com/segmentio/kafka-go"
)

func main() {
	db, err := ConnectDB()
	if err != nil {
		log.Fatal("Database connection failed:", err)
	}
	defer db.Close()

	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	go runPublisher(context.Background(), db, writer)

	http.HandleFunc("/jobs", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			CreateJob(w, r, db)

		case http.MethodGet:
			GetJobs(w, r, db)

		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	http.HandleFunc("GET /jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		GetJobByID(w, r, db)
	})
	http.HandleFunc("PUT /jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		UpdateJob(w, r, db)
	})
	http.HandleFunc("DELETE /jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		DeleteJob(w, r, db)
	})

	log.Println("Server running on :8000")
	log.Fatal(http.ListenAndServe(":8000", nil))
}

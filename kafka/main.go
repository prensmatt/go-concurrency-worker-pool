package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
)

func main() {
	db, err := ConnectDB()
	if err != nil {
		log.Fatal("Database connection failed:", err)
	}
	defer db.Close()

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	writer := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Balancer: &kafka.LeastBytes{},
	}
	defer writer.Close()

	var wg sync.WaitGroup
	shutdownDone := make(chan struct{})

	wg.Add(2)
	go func() {
		defer wg.Done()
		runPublisher(ctx, db, writer)
	}()
	go func() {
		defer wg.Done()
		runConsumer(ctx)
	}()

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

	server := &http.Server{
		Addr:    ":8000",
		Handler: nil,
	}

	log.Println("Server running on :8000")

	//server shutdown
	go func() {
		defer close(shutdownDone)

		<-ctx.Done()

		log.Println("Shutting down HTTP server...")

		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := server.Shutdown(shutdownCtx); err != nil {
			log.Println("HTTP server shutdown failed:", err)
		}
	}()

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Println("Server error:", err)
		stop()
	}
	wg.Wait()
	<-shutdownDone

	log.Println("Application shutdown complete")
}

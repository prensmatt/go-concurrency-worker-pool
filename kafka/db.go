package main

import (
	"context"
  "database/sql"
	"fmt"
	"os"

	"github.com/joho/godotenv"
    _ "github.com/jackc/pgx/v5/stdlib"
)

func ConnectDB() (*sql.DB, error){

	err := godotenv.Load()
	if err != nil{
		return nil, err
	}


	 connStr := fmt.Sprintf(
    "postgres://%s:%s@localhost:%s/%s?sslmode=disable",
    os.Getenv("POSTGRES_USER"),
    os.Getenv("POSTGRES_PASSWORD"),
    os.Getenv("POSTGRES_PORT"),
    os.Getenv("POSTGRES_DB"),
)

	 db, err := sql.Open("pgx", connStr)
    if err != nil {
        return nil, err
    }

		ctx := context.Background()

		if err := db.PingContext(ctx); err != nil{
			db.Close()
			return nil, err
		}

    return db, nil
}
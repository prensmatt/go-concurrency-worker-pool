package main

import(
	"fmt"
	"log"
	"net/http"
)

func main(){
	db, err := ConnectDB()
	if err != nil{
		log.Fatal("Database connection failed:", err)
	}
	defer db.Close()

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request){
		fmt.Fprintln(w,"API is running!")
	})

	log.Println("Server running on :8000")
	log.Fatal(http.ListenAndServe(":8000", nil))
}
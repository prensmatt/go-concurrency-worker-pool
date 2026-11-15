package main

import(
	"log"
	"net/http"
)

func main(){
	db, err := ConnectDB()
	if err != nil{
		log.Fatal("Database connection failed:", err)
	}
	defer db.Close()

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

	log.Println("Server running on :8000")
	log.Fatal(http.ListenAndServe(":8000", nil))
}
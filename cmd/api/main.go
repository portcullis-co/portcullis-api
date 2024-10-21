package main

import (
	"log"
	"net/http"

	"github.com/portcullis-co/portcullis-api/internal/api"
)

func main() {
	http.HandleFunc("/extract", api.ExtractHandler)
	http.HandleFunc("/load", api.LoadHandler)

	log.Println("Starting server on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
}

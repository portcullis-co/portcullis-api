package main

import (
	"log"
	"net/http"
)

func main() {
	http.HandleFunc("/transfer", TransferHandler)

	log.Println("Starting server on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
}

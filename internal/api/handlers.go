package api

import (
	"encoding/json"
	"net/http"

	"github.com/portcullis-co/portcullis-api/internal/datasource"
	"github.com/portcullis-co/portcullis-api/internal/types"
)

func ExtractHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req types.ExtractRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	data, err := datasource.Extract(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

func LoadHandler(w http.ResponseWriter, r *http.Request) {
	// ... similar implementation to ExtractHandler
}

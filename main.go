package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"strings"
)

func main() {
	apiKey := os.Getenv("INFRAI_API_KEY")
	if apiKey == "" {
		log.Fatal("INFRAI_API_KEY is required")
	}
	queue := NewQueueClient(apiKey)
	worker := Worker{queue: queue, process: processGameJob}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /jobs", func(w http.ResponseWriter, r *http.Request) {
		var job GameJob
		if err := json.NewDecoder(r.Body).Decode(&job); err != nil || job.JobID == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "valid job_id and JSON body required"})
			return
		}
		job.Attempt = 0
		job.DeadLetter = false
		job.FailureClass = ""
		if err := queue.Publish(r.Context(), job, publishKey("submit", job)); err != nil {
			writeServiceError(w, err)
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]string{"job_id": job.JobID, "state": "queued"})
	})
	mux.HandleFunc("POST /work", func(w http.ResponseWriter, r *http.Request) {
		result, err := worker.RunOnce(r.Context())
		if err != nil {
			writeServiceError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
	})

	addr := os.Getenv("ADDR")
	if addr == "" {
		addr = ":8080"
	}
	log.Printf("game queue worker listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func writeServiceError(w http.ResponseWriter, err error) {
	status := http.StatusBadGateway
	var apiErr *InfraiError
	if errors.As(err, &apiErr) && apiErr.HTTPStatus >= 400 && apiErr.HTTPStatus < 500 {
		status = apiErr.HTTPStatus
	}
	writeJSON(w, status, map[string]string{"error": strings.TrimSpace(err.Error())})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("encode response: %v", err)
	}
}

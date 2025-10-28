package syncasyncreplicationprototype

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
)

var replicas = []string{
	"http://localhost:3000",
	"http://localhost:3001",
	"http://localhost:3002",
}

type RequestData struct {
	Name     string `json:"name"`
	UserID   int    `json:"user_id"`
	IsActive bool   `json:"is_active"`
}

func parseRequest(r *http.Request) (RequestData, error) {
	var data RequestData
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return data, err
	}
	defer r.Body.Close()

	err = json.Unmarshal(body, &data)
	return data, err
}

func writeToLocalDb(data RequestData) error {
	file, err := os.OpenFile("localdb.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	record, _ := json.Marshal(data)
	_, err = file.Write(append(record, '\n'))
	return err
}

func AsyncReplicationHandler(w http.ResponseWriter, r *http.Request) {
	data, err := parseRequest(r)
	if err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	err = writeToLocalDb(data)
	if err != nil {
		http.Error(w, "Failed to write locally", http.StatusInternalServerError)
		return
	}

	queryBytes, _ := json.Marshal(data)
	go replicateToReplica(string(queryBytes))

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Write successful (replication in progress)"))
}

func replicateToReplica(query string) {
	for _, replica := range replicas {
		go sendToReplica(replica, query)
	}
}

func sendToReplica(replica string, query string) {
	resp, err := http.Post(replica+"/replicate", "application/json", strings.NewReader(query))
	if err != nil {
		log.Printf("Failed to replicate to %s: %v", replica, err)
		return
	}
	defer resp.Body.Close()
	log.Printf("Replicated to %s successfully", replica)
}

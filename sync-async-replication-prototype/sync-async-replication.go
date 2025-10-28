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

func sendToReplica(replica string, query string) bool {
	resp, err := http.Post(replica+"/replicate", "application/json", strings.NewReader(query))
	if err != nil {
		log.Printf("Failed to replicate to %s: %v", replica, err)
		return false
	}
	defer resp.Body.Close()
	log.Printf("Replicated to %s successfully", replica)
	return true
}

// ~ so basically now over ther I have to handle the functionality of the sync replication so as this have to be done with in the blocking way so with in that I have to handle that out using the buiffered channels I think so

func SyncReplicationHandler(w http.ResponseWriter, r *http.Request) {
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

	// ~ so here I have to create the acknowledgement channel but over there the unbuffered channels are created \

	ackCh := make(chan bool)

	queryBytes, _ := json.Marshal(data)

	for _, replica := range replicas {
		go func(replica string) {
			ack := sendToReplica(replica, string(queryBytes))
			ackCh <- ack
		}(replica)
	}

	successCount := 0

	for range replicas {
		if <-ackCh {
			successCount++
		}
	}

	if successCount == len(replicas) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("✅ All replicas confirmed. Write committed."))

	} else {
		http.Error(w, "⚠️ Some replicas failed to confirm.", http.StatusGatewayTimeout)
	}

}

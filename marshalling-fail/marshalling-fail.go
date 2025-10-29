package marshallingfail

import (
	"encoding/json"
	"fmt"
)

type GithubActions struct {
	ID int32 `json:"id"`
}

func MarshallingFail() {
	// Simulating incoming JSON payload
	jsonData := []byte(`{"id": 2147483648}`) // exceeds 32-bit limit

	var data GithubActions
	err := json.Unmarshal(jsonData, &data)
	if err != nil {
		fmt.Println("❌ JSON unmarshal error:", err)
		return
	}

	fmt.Println("✅ Parsed:", data)
}

package mr

import (
	"math/rand"
	"time"
)

const charset = "123456789abcdefghijklmnopqrstuvwxyz"

func generateRandomString(length int) string {
	rand.New(rand.NewSource(time.Now().UnixNano()))

	result := make([]byte, length)
	for i := range result {
		result[i] = charset[rand.Intn(len(charset))]
	}

	return string(result)
}

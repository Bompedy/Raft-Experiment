package shared

import (
	"net"
	"os"
	"strconv"
	"sync"
)

type Client struct {
	Connection net.Conn
	Mutex      *sync.Mutex
}

func (client Client) Read(buffer []byte) error {
	for start := 0; start != len(buffer); {
		amount, reason := client.Connection.Read(buffer[start:])
		if reason != nil {
			return reason
		}
		start += amount
	}
	return nil
}
func (client Client) Write(buffer []byte) error {
	for start := 0; start != len(buffer); {
		amount, reason := client.Connection.Write(buffer[start:])
		if reason != nil {
			return reason
		}
		start += amount
	}
	return nil
}

func GetEnvInt(key string, defaultValue int) int {
	intValue, err := strconv.Atoi(os.Getenv(key))
	if err != nil {
		return defaultValue
	}
	if defaultValue < 1 {
		return defaultValue
	}
	return intValue
}

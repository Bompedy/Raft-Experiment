package shared

import (
	"net"
	"os"
	"strconv"
)

func Read(connection net.Conn, buffer []byte) error {
	for start := 0; start != len(buffer); {
		amount, reason := connection.Read(buffer[start:])
		if reason != nil {
			return reason
		}
		start += amount
	}
	return nil
}
func Write(connection net.Conn, buffer []byte) error {
	for start := 0; start != len(buffer); {
		amount, reason := connection.Write(buffer[start:])
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

package client

import (
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"os"
	"raftlib/shared"
	"sync"
	"sync/atomic"
	"time"
)

func Client() {
	numThreads := shared.GetEnvInt("NUM_THREADS", 1)
	//numNodes := shared.GetEnvInt("NUM_NODES", 1)
	numClients := shared.GetEnvInt("NUM_CLIENTS", 1)
	dataSize := shared.GetEnvInt("DATA_SIZE", 1)
	numOps := shared.GetEnvInt("NUM_OPS", 1)
	leaderNodeAddress := os.Getenv("LEADER_NODE_ADDRESS")
	//peerNodeAddresses := strings.Split(os.Getenv("PEER_NODE_ADDRESSES"), ",")

	fmt.Println("=== Client Configuration ===")
	fmt.Printf("NUM_THREADS:              %d\n", numThreads)
	//fmt.Printf("NUM_NODES:                %d\n", numNodes)
	fmt.Printf("NUM_CLIENTS:              %d\n", numClients)
	fmt.Printf("DATA_SIZE:                %d\n", dataSize)
	fmt.Printf("NUM_OPS:                  %d\n", numOps)
	fmt.Printf("LEADER_NODE_ADDRESS:      %v\n", leaderNodeAddress)

	connections := make([]*shared.Client, numClients)
	data := make([]byte, dataSize)

	for i := 0; i < numClients; i++ {
		for {
			//fmt.Printf("Connecting to %s\n", address)
			conn, err := net.Dial("tcp", leaderNodeAddress)
			if err != nil {
				time.Sleep(1 * time.Second)
				continue
			}
			//fmt.Printf("Connected to %s\n", address)
			connections[i] = &shared.Client{Connection: conn, Mutex: &sync.Mutex{}}
			break
		}
	}

	connectionIndex := int32(0)
	remaining := numOps
	perThread := numOps / numThreads
	for index := 0; index < numThreads; index++ {
		leftover := 0
		remaining -= perThread
		if remaining < perThread {
			leftover = remaining
			remaining = 0
		}
		fmt.Printf("Thread %d performing %d ops", index, perThread+leftover)
		go func(i int) {
			sizeBuffer := make([]byte, 4)
			for op := 0; op < perThread+leftover; op++ {
				connection := connections[atomic.AddInt32(&connectionIndex, 1)-1]
				binary.LittleEndian.PutUint32(sizeBuffer, uint32(dataSize))
				err := connection.Write(sizeBuffer)
				if err != nil {
					log.Printf("Error sending message: %v", err)
					return
				}
				err = connection.Write(data)
				if err != nil {
					log.Printf("Error sending message: %v", err)
					return
				}
				//do a read here and convert to for {}
			}
		}(index)
	}
}

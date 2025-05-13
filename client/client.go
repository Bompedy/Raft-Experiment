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

const CLOSE_LOOP = true

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
	//fmt.Printf("data: %d(%s)\n", binary.LittleEndian.Uint16(writeBuffer[:8]), string(writeBuffer[8:]))

	//mutex := sync.Mutex{}
	responses := int32(0)

	for i := 0; i < numClients; i++ {
		for {
			//fmt.Printf("Connecting to %s\n", address)
			conn, err := net.Dial("tcp", leaderNodeAddress)
			if err != nil {
				time.Sleep(1 * time.Second)
				continue
			}
			//fmt.Printf("Connected to %s\n", address)
			client := &shared.Client{Connection: conn, Mutex: &sync.Mutex{}}
			connections[i] = client
			break
		}
	}
	start := time.Now().UnixMilli()
	for i := 0; i < numClients; i++ {
		connection := connections[i]
		go func(i int, connection *shared.Client) {
			buffer := make([]byte, 8)
			for {
				err := connection.Read(buffer)
				if err != nil {
					fmt.Println("Read error")
					return
				}
				//fmt.Printf("Response %d\n", binary.LittleEndian.Uint64(buffer))
				if CLOSE_LOOP {
					connection.Mutex.Unlock()
					count := atomic.AddInt32(&responses, 1)
					if count%1000 == 0 {
						fmt.Printf("%d\n", count)
					}
					if count == int32(numOps) {
						seconds := float64(time.Now().UnixMilli()-start) / 1000.0
						fmt.Printf("%f ops in %f seconds\n", float64(numOps)/seconds, seconds)
					}
				} else {
					//TODO
				}
			}
		}(i, connection)
	}

	connectionIndex := int32(0)
	remaining := numOps
	perThread := numOps / numThreads

	messageId := uint64(0)
	for i := 0; i < numThreads; i++ {
		leftover := 0
		remaining -= perThread
		if remaining < perThread {
			leftover = remaining
			remaining = 0
		}
		fmt.Printf("Thread %d performing %d ops\n", i, perThread+leftover)
		go func(i int) {
			sizeBuffer := make([]byte, 4)
			buffer := make([]byte, dataSize+8)
			for op := 0; op < perThread+leftover; op++ {
				current := atomic.AddInt32(&connectionIndex, 1) - 1
				connection := connections[current%int32(numClients)]
				if CLOSE_LOOP {
					connection.Mutex.Lock()
				} else {
					//TODO
				}
				binary.LittleEndian.PutUint32(sizeBuffer, uint32(dataSize+8))
				err := connection.Write(sizeBuffer)
				if err != nil {
					log.Printf("Error sending message: %v", err)
					return
				}
				//messageId := r.Uint64()
				//fmt.Printf("Sending %d\n", messageId)
				currentMessageId := atomic.AddUint64(&messageId, 1) - 1
				binary.LittleEndian.PutUint64(buffer, currentMessageId)
				err = connection.Write(buffer)
				if err != nil {
					log.Printf("Error sending message: %v", err)
					return
				}
			}
		}(i)
	}

	for {
		continue
	}
}

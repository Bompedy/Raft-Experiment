package client

import (
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
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
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	connections := make([]*shared.Client, numClients)
	//fmt.Printf("data: %d(%s)\n", binary.LittleEndian.Uint16(writeBuffer[:8]), string(writeBuffer[8:]))

	mutex := sync.Mutex{}

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
			go func(i int, connection *shared.Client) {
				buffer := make([]byte, 8)
				for {
					err := connection.Read(buffer)
					if err != nil {
						fmt.Println("Read error")
						return
					}
					fmt.Printf("Response %d\n", binary.LittleEndian.Uint64(buffer))
					if CLOSE_LOOP {
						mutex.Unlock()
					} else {
						//TODO
					}
				}
			}(i, client)
			break
		}
	}

	connectionIndex := int32(0)
	remaining := numOps
	perThread := numOps / numThreads

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
				if CLOSE_LOOP {
					mutex.Lock()
				} else {
					//TODO
				}
				current := atomic.AddInt32(&connectionIndex, 1) - 1
				connection := connections[current%int32(numClients)]
				binary.LittleEndian.PutUint32(sizeBuffer, uint32(dataSize+8))
				connection.Mutex.Lock()
				err := connection.Write(sizeBuffer)
				connection.Mutex.Unlock()
				if err != nil {
					log.Printf("Error sending message: %v", err)
					return
				}
				messageId := r.Uint64()
				fmt.Printf("Sending %d\n", messageId)
				binary.LittleEndian.PutUint64(buffer, messageId)
				connection.Mutex.Lock()
				err = connection.Write(buffer)
				connection.Mutex.Unlock()
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

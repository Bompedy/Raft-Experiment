package client

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"raftlib/shared"
	"sync"
	"sync/atomic"
	"time"
)

func Client() {
	closedLoop := os.Getenv("CLOSED_LOOP")
	if closedLoop != "true" && closedLoop != "false" {
		panic("Must specify CLOSED_LOOP: (true|false)")
	}

	numThreads := shared.GetEnvInt("NUM_THREADS", 1)
	numClients := shared.GetEnvInt("NUM_CLIENTS", 1)
	dataSize := shared.GetEnvInt("DATA_SIZE", 1)
	numOps := shared.GetEnvInt("NUM_OPS", 1)
	leaderNodeAddress := os.Getenv("LEADER_NODE_ADDRESS")

	fmt.Println("=== Client Configuration ===")
	fmt.Printf("CLOSED_LOOP:              %s\n", closedLoop)
	fmt.Printf("NUM_THREADS:              %d\n", numThreads)
	fmt.Printf("NUM_CLIENTS:              %d\n", numClients)
	fmt.Printf("DATA_SIZE:                %d\n", dataSize)
	fmt.Printf("NUM_OPS:                  %d\n", numOps)
	fmt.Printf("LEADER_NODE_ADDRESS:      %v\n", leaderNodeAddress)
	connections := make([]*shared.Client, numClients)

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

	if closedLoop == "true" {
		opsPerThread := numOps / numThreads
		var startGroup sync.WaitGroup
		var completeGroup sync.WaitGroup
		startGroup.Add(numThreads)
		completeGroup.Add(numThreads)

		for i := 0; i < numThreads; i++ {
			go func(threadId int) {
				connectionIndex := threadId % numClients
				startOp := threadId * opsPerThread
				buffer := make([]byte, dataSize+8)
				responseBuffer := make([]byte, 4)
				startGroup.Done()
				startGroup.Wait()

				for op := range opsPerThread {
					binary.LittleEndian.PutUint32(buffer, uint32(dataSize+4))
					binary.LittleEndian.PutUint32(buffer[4:], uint32(startOp+op))
					connection := connections[connectionIndex]
					connectionIndex = (connectionIndex + 1) % numClients
					connection.Mutex.Lock()
					err := connection.Write(buffer)
					if err != nil {
						panic(err)
					}
					err = connection.Read(responseBuffer)
					connection.Mutex.Unlock()
					if err != nil {
						panic(err)
					}
				}

				completeGroup.Done()
			}(i)
		}

		startGroup.Wait()
		start := time.Now()
		fmt.Printf("Starting benchmark!\n")
		completeGroup.Wait()

		total := time.Since(start)
		ops := float64(numOps) / total.Seconds()
		fmt.Printf("Total clients: %d\n", numClients)
		fmt.Printf("Total operations: %d\n", numOps)
		fmt.Printf("Total time taken: %v\n", total)
		fmt.Printf("Data size: %d bytes\n", dataSize)
		fmt.Printf("OPS: %.4f ops/sec\n", ops)
	} else {
		completeChannel := make(chan struct{})
		var completedOps atomic.Uint64
		for i := range connections {
			connection := connections[i]
			go func() {
				for {
					buffer := make([]byte, 4)
					err := connection.Read(buffer)
					if err != nil {
						panic(err)
					}
					//fmt.Printf("Client %d: Read %d\n", i, len(buffer))
					completed := completedOps.Add(1)
					if completed%10000 == 0 {
						fmt.Printf("%d\n", completed)
					}
					if completed == uint64(numOps) {
						close(completeChannel)
					}
				}
			}()
		}

		opsPerThread := numOps / numThreads
		var startGroup sync.WaitGroup
		startGroup.Add(numThreads)

		for i := 0; i < numThreads; i++ {
			go func(threadId int) {
				connectionIndex := threadId % numClients
				startOp := threadId * opsPerThread
				buffer := make([]byte, dataSize+8)
				startGroup.Done()
				startGroup.Wait()

				for op := range opsPerThread {
					binary.LittleEndian.PutUint32(buffer, uint32(dataSize+4))
					binary.LittleEndian.PutUint32(buffer[4:], uint32(startOp+op))
					connection := connections[connectionIndex]
					connectionIndex = (connectionIndex + 1) % numClients
					connection.Mutex.Lock()
					err := connection.Write(buffer)
					if err != nil {
						panic(err)
					}
					connection.Mutex.Unlock()
				}
			}(i)
		}

		startGroup.Wait()
		start := time.Now()
		fmt.Printf("Starting benchmark!\n")

		<-completeChannel
		total := time.Since(start)
		ops := float64(numOps) / total.Seconds()
		fmt.Printf("Total clients: %d\n", numClients)
		fmt.Printf("Total operations: %d\n", numOps)
		fmt.Printf("Total time taken: %v\n", total)
		fmt.Printf("Data size: %d bytes\n", dataSize)
		fmt.Printf("OPS: %.4f ops/sec\n", ops)
	}
}

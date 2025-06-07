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

import _ "net/http/pprof"

type ClientConnection struct {
	connection *net.Conn
	channel    *chan []byte
}

func Client() {
	closedLoop := os.Getenv("CLOSED_LOOP")
	if closedLoop != "true" && closedLoop != "false" {
		panic("Must specify CLOSED_LOOP: (true|false)")
	}

	numThreads := shared.GetEnvInt("NUM_THREADS", 1)
	numClients := shared.GetEnvInt("NUM_CLIENTS", 1)
	dataSize := shared.GetEnvInt("DATA_SIZE", 1)
	poolWarmupSize := shared.GetEnvInt("POOL_WARMUP_SIZE", 1)
	numOps := shared.GetEnvInt("NUM_OPS", 1)
	leaderNodeAddress := os.Getenv("LEADER_NODE_ADDRESS")

	var pool = sync.Pool{
		New: func() interface{} {
			return make([]byte, dataSize+8)
		},
	}

	for range poolWarmupSize {
		pool.Put(pool.Get())
	}

	fmt.Println("=== Client Configuration ===")
	fmt.Printf("CLOSED_LOOP:              %s\n", closedLoop)
	fmt.Printf("NUM_THREADS:              %d\n", numThreads)
	fmt.Printf("NUM_CLIENTS:              %d\n", numClients)
	fmt.Printf("DATA_SIZE:                %d\n", dataSize)
	fmt.Printf("NUM_OPS:                  %d\n", numOps)
	fmt.Printf("LEADER_NODE_ADDRESS:      %v\n", leaderNodeAddress)
	connections := make([]*ClientConnection, numClients)
	for i := 0; i < numClients; i++ {
		for {
			conn, err := net.Dial("tcp", leaderNodeAddress)
			if err != nil {
				time.Sleep(1 * time.Second)
				continue
			}
			err = conn.(*net.TCPConn).SetNoDelay(true)
			if err != nil {
				panic("error setting no delay")
			}
			writeChan := make(chan []byte, 3000000)

			connections[i] = &ClientConnection{&conn, &writeChan}
			go writeChannel(&pool, conn, writeChan)
			break
		}
	}

	if closedLoop == "true" {
	} else {
		completeChannel := make(chan struct{})
		var completedOps atomic.Uint64
		for i := range connections {
			client := connections[i]
			connection := *client.connection
			go func() {
				buffer := make([]byte, 4)
				for {
					err := shared.Read(connection, buffer[:4])
					if err != nil {
						panic(err)
					}

					//fmt.Printf("Client %d: Read %d\n", binary.LittleEndian.Uint32(buffer[:4]), len(buffer))
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

		opsPerClient := numOps / numClients
		var startGroup sync.WaitGroup
		startGroup.Add(numClients)

		for i := range numClients {
			go func(clientId int) {
				startOp := i * opsPerClient
				startGroup.Done()
				startGroup.Wait()

				for op := range opsPerClient {
					buffer := pool.Get().([]byte)
					binary.LittleEndian.PutUint32(buffer, uint32(4))
					binary.LittleEndian.PutUint32(buffer[4:], uint32(startOp+op))
					if startOp+op > numOps {
						fmt.Printf("clientId:%d op:%d\n", clientId, startOp+op)
					}
					//fmt.Printf("%d: %d\n", threadId, startOp+op)

					client := connections[clientId]
					channel := *client.channel
					select {
					case channel <- buffer[:8]:
					default:
						log.Printf("Client write channel is full, dropping message?")
					}
				}
			}(i)
		}

		//for i := 0; i < numThreads; i++ {
		//	go func(threadId int) {
		//		//connectionIndex := threadId % numClients
		//		startOp := threadId * opsPerClient
		//
		//		//buffer := make([]byte, dataSize+8)
		//		startGroup.Done()
		//		startGroup.Wait()
		//
		//		for op := range opsPerClient {
		//			buffer := pool.Get().([]byte)
		//			binary.LittleEndian.PutUint32(buffer, uint32(4))
		//			binary.LittleEndian.PutUint32(buffer[4:], uint32(startOp+op))
		//			//fmt.Printf("%d: %d\n", threadId, startOp+op)
		//
		//			client := connections[connectionIndex]
		//			connectionIndex = (connectionIndex + 1) % numClients
		//			channel := *client.channel
		//			select {
		//			case channel <- buffer[:8]:
		//			default:
		//				log.Printf("Client write channel is full, dropping message?")
		//			}
		//		}
		//	}(i)
		//}

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

func writeChannel(pool *sync.Pool, conn net.Conn, channel chan []byte) {
	defer func() {
		err := conn.Close()
		if err != nil {
			panic(err)
		}
		close(channel)
	}()

	for msg := range channel {
		err := shared.Write(conn, msg)
		//first := binary.LittleEndian.Uint32(msg[:4])
		//second := binary.LittleEndian.Uint32(msg[4:8])
		if err != nil {
			panic(err)
		}
		pool.Put(msg)
	}
}

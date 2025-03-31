package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {
	if os.Getenv("CLIENT") == "true" {
		numThreads, err := strconv.Atoi(os.Getenv("NUM_NODES"))
		if err != nil {
			panic(err)
		}
		if numThreads < 1 {
			numThreads = 1
		}

		numClients, err := strconv.Atoi(os.Getenv("NUM_CLIENTS"))
		if err != nil {
			panic(err)
		}
		if numClients < 1 {
			numClients = 1
		}

		dataSize, err := strconv.Atoi(os.Getenv("DATA_SIZE"))
		if err != nil {
			panic(err)
		}
		if dataSize < 1 {
			panic("DATA SIZE MUST BE LARGER THAN 0")
		}

		numOps, err := strconv.Atoi(os.Getenv("NUM_OPS"))
		if err != nil {
			panic(err)
		}
		if numOps < 1 {
			panic("NUMBER OF OPS MUST BE LARGER THAN 0")
		}

		peerNodeAddresses := strings.Split(os.Getenv("PEER_NODE_ADDRESSES"), ",")
		fmt.Printf("peer_node_addresses = %s\n", peerNodeAddresses)

	} else {
		numNodes, err := strconv.Atoi(os.Getenv("NUM_NODES"))
		if err != nil {
			panic(err)
		}
		if numNodes < 1 {
			numNodes = 1
		}

		nodeId, err := strconv.Atoi(os.Getenv("NODE_ID"))
		if err != nil {
			panic(err)
		}
		if nodeId < 1 {
			panic("NODE_ID must be greater than zero")
		}

		numPeerConnections, err := strconv.Atoi(os.Getenv("NUM_PEER_CONNECTIONS"))
		if err != nil {
			panic(err)
		}

		if numPeerConnections < 1 {
			numPeerConnections = 1
		}

		numPeerClientConnections, err := strconv.Atoi(os.Getenv("NUM_PEER_CLIENT_CONNECTIONS"))
		if err != nil {
			panic(err)
		}

		if numPeerClientConnections < 1 {
			numPeerClientConnections = 1
		}

		hostNodeAddress := os.Getenv("HOST_NODE_ADDRESS")
		clientNodeAddress := os.Getenv("CLIENT_NODE_ADDRESS")
		peerNodeAddresses := strings.Split(os.Getenv("PEER_NODE_ADDRESSES"), ",")
		peerClientAddresses := strings.Split(os.Getenv("PEER_CLIENT_ADDRESSES"), ",")

		fmt.Printf("host_node_address = %s\n", hostNodeAddress)
		fmt.Printf("client_node_address = %s\n", clientNodeAddress)
		fmt.Printf("peer_node_addresses = %s\n", peerNodeAddresses)
		fmt.Printf("peer_client_addresses = %s\n", peerClientAddresses)
		fmt.Printf("num_peer_connections = %d\n", numPeerConnections)
		fmt.Printf("num_nodes = %d\n", numNodes)
		fmt.Printf("node_id = %d\n", nodeId)

		clientListener, err := net.Listen("tcp", clientNodeAddress)
		if err != nil {
			panic(err)
		}

		defer clientListener.Close()

		peerListener, err := net.Listen("tcp", hostNodeAddress)
		if err != nil {
			panic(err)
		}

		defer peerListener.Close()

		var waitGroup sync.WaitGroup
		waitGroup.Add((numNodes * numPeerConnections) - numPeerConnections)

		store := raft.NewMemoryStorage()
		config := &raft.Config{
			ID:              uint64(nodeId),
			ElectionTick:    10,
			HeartbeatTick:   1,
			Storage:         store,
			MaxSizePerMsg:   4096,
			MaxInflightMsgs: 256,
		}
		peers := make([]raft.Peer, numNodes)
		for i := 0; i < numNodes; i++ {
			peers[i].ID = uint64(i + 1)
		}
		for i := range peers {
			fmt.Printf("%v\n", peers[i])
		}
		node := raft.StartNode(config, peers)

		peerConnections := make([][]*Client, numNodes)
		for i := range peerConnections {
			peerConnections[i] = make([]*Client, numPeerConnections)
		}

		for i, nodeAddress := range peerNodeAddresses {
			for range peerConnections {
				for {
					if nodeAddress == hostNodeAddress {
						break
					}
					connection, err := net.Dial("tcp", nodeAddress)
					if err != nil {
						continue
					}
					fmt.Printf("connected to %s\n", nodeAddress)
					client := &Client{connection: connection, mutex: &sync.Mutex{}}
					peerConnections[i] = append(peerConnections[i], client)
					break
				}
			}
		}

		go func() {
			for {
				connection, err := clientListener.Accept()
				if err != nil {
					panic(err)
				}
				client := &Client{connection: connection, mutex: &sync.Mutex{}}
				go func() {
					sizeBuffer := make([]byte, 4)
					readBuffer := make([]byte, config.MaxSizePerMsg)
					for {
						err := client.Read(sizeBuffer)
						if err != nil {
							panic(err)
						}

						amount := binary.LittleEndian.Uint32(sizeBuffer)
						err = client.Read(readBuffer[:amount])
						if err != nil {
							panic(err)
						}

						leadId := node.Status().Lead
						if leadId == config.ID {
							err := node.Propose(context.TODO(), readBuffer[:amount])
							if err != nil {
								panic(err)
							}
							go func() {

							}()
							// need to wait to write back with channel

						} else {
							panic("NON LEADER GOT CLIENT MESSAGE")
						}
					}
				}()
			}
		}()

		go func() {
			for {
				connection, err := peerListener.Accept()
				if err != nil {
					panic(err)
				}
				client := &Client{connection: connection, mutex: &sync.Mutex{}}
				go func() {
					waitGroup.Done()
					sizeBuffer := make([]byte, 4)
					readBuffer := make([]byte, config.MaxSizePerMsg)
					for {
						fmt.Printf("%d - %d\n", config.ID, node.Status().Lead)
						err := client.Read(sizeBuffer)
						if err != nil {
							panic(err)
						}

						amount := binary.LittleEndian.Uint32(sizeBuffer[:])
						err = client.Read(readBuffer[:amount])
						if err != nil {
							panic(err)
						}

						var msg raftpb.Message
						err = msg.Unmarshal(readBuffer[:amount])
						if err != nil {
							panic(err)
						}

						fmt.Printf("msg = %v\n", msg)
						err = node.Step(context.TODO(), msg)
						if err != nil {
							panic(err)
						}
					}
				}()
			}
		}()

		waitGroup.Wait()

		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		go func() {
			if node.Status().Lead == config.ID {
				time.Sleep(5 * time.Second)
			}
		}()

		for {
			select {
			case <-ticker.C:
				node.Tick()
			case rd := <-node.Ready():

				if len(rd.Entries) > 0 {
					fmt.Printf("Appending entries: %d\n", rd.Entries)
					for i := 0; i < len(rd.Entries); i++ {
						fmt.Printf("entry: %v\n", string(rd.Entries[i].Data[:]))
					}
					err := store.Append(rd.Entries)
					if err != nil {
						panic(err)
					}
				}

				for _, msg := range rd.Messages {
					log.Println("Sending message:", msg)
					bytes, err := msg.Marshal()
					if err != nil {
						panic(err)
					}
					amountBuffer := make([]byte, 4)
					binary.LittleEndian.PutUint32(amountBuffer, uint32(len(bytes)))

					connection := peerConnections[msg.To-1][0]
					err = connection.Write(amountBuffer[:])
					if err != nil {
						panic(err)
					}

					err = connection.Write(bytes[:])
					if err != nil {
						panic(err)
					}
				}

				if !raft.IsEmptySnap(rd.Snapshot) {
					fmt.Printf("Applying snapshot...\n")
				}

				for _, entry := range rd.CommittedEntries {
					fmt.Printf("committed entry: %v\n", entry)
					if entry.Type == raftpb.EntryConfChange {
						fmt.Printf("Apply change\n")
						var cc raftpb.ConfChange
						err := cc.Unmarshal(entry.Data)
						if err != nil {
							panic(err)
						}
						node.ApplyConfChange(cc)
					} else if entry.Type == raftpb.EntryNormal {
						// apply to state machine
					}
				}
				node.Advance()
			}
		}
	}
}

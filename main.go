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
	numNodes, err := strconv.Atoi(os.Getenv("NUM_NODES"))
	if err != nil {
		panic(err)
	}
	if numNodes < 1 {
		panic("NUM_NODES must be greater than zero")
	}

	nodeId, err := strconv.Atoi(os.Getenv("NODE_ID"))
	if err != nil {
		panic(err)
	}
	if nodeId < 1 {
		panic("NODE_ID must be greater than zero")
	}

	hostNodeAddress := os.Getenv("HOST_NODE_ADDRESS")
	peerNodeAddresses := strings.Split(os.Getenv("PEER_NODE_ADDRESSES"), ",")
	fmt.Printf("host_node_address = %s\n", hostNodeAddress)
	fmt.Printf("peer_node_addresses = %s\n", peerNodeAddresses)
	fmt.Printf("num_nodes = %d\n", numNodes)
	fmt.Printf("node_id = %d\n", nodeId)

	listener, err := net.Listen("tcp", hostNodeAddress)
	if err != nil {
		panic(err)
	}

	defer listener.Close()

	var waitGroup sync.WaitGroup
	waitGroup.Add(numNodes - 1)

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

	peerConnections := make([]*Client, numNodes)
	for i := range peerNodeAddresses {
		for {
			address := peerNodeAddresses[i]
			if address == hostNodeAddress {
				fmt.Printf("found ourselves")
				break
			}
			connection, err := net.Dial("tcp", address)
			if err != nil {
				continue
			}
			fmt.Printf("connected to %s\n", address)
			peerConnections[i] = &Client{connection: connection, mutex: &sync.Mutex{}}
			break
		}
	}

	go func() {
		for {
			connection, err := listener.Accept()
			if err != nil {
				panic(err)
			}
			client := &Client{connection: connection, mutex: &sync.Mutex{}}
			go func() {
				waitGroup.Done()
				sizeBuffer := make([]byte, 4)
				readBuffer := make([]byte, config.MaxSizePerMsg)
				for {
					err := client.Read(sizeBuffer[:4])
					if err != nil {
						panic(err)
					}
					amount := binary.LittleEndian.Uint32(sizeBuffer)
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
	fmt.Printf("Got all connections!")

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	go func() {
		i := 0
		for {
			data := []byte(fmt.Sprintf("message-%d", i))
			err := node.Propose(context.TODO(), data)
			if err != nil {
				log.Println("Failed to propose:", err)
			} else {
				log.Println("Proposed:", string(data))
			}
			time.Sleep(1 * time.Second)
			i++
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

				connection := peerConnections[msg.To-1]
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
				}
			}
			node.Advance()
		}
	}
}

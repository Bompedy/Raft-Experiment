package server

import (
	"math"
	"net/http"
	_ "net/http/pprof"
)
import (
	"context"
	"encoding/binary"
	"fmt"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"log"
	"net"
	"os"
	"raftlib/shared"
	"strings"
	"sync"
	"time"
)

type RaftServer struct {
	nodeID                   int
	numNodes                 int
	numPeerConnections       int
	numPeerClientConnections int
	dataSize                 int
	hostNodeAddress          string
	clientNodeAddress        string
	peerNodeAddresses        []string
	peerConnections          [][]*shared.Client
	senders                  sync.Map
	node                     raft.Node
	storage                  *raft.MemoryStorage
	config                   *raft.Config
	waitGroup                sync.WaitGroup
}

func NewRaftServer() *RaftServer {
	peerAddresses := strings.Split(os.Getenv("PEER_NODE_ADDRESSES"), ",")
	s := &RaftServer{
		numNodes:           len(peerAddresses),
		nodeID:             shared.GetEnvInt("NODE_ID", 1),
		hostNodeAddress:    os.Getenv("HOST_NODE_ADDRESS"),
		clientNodeAddress:  os.Getenv("CLIENT_NODE_ADDRESS"),
		peerNodeAddresses:  peerAddresses,
		numPeerConnections: shared.GetEnvInt("NUM_PEER_CONNECTIONS", 1),
		dataSize:           shared.GetEnvInt("DATA_SIZE", 1),
		storage:            raft.NewMemoryStorage(),
	}

	go func() {
		log.Println(http.ListenAndServe("10.10.1.1:6060", nil))
	}()

	fmt.Println("=== Server Configuration ===")
	fmt.Printf("NODE_ID:                     %d\n", s.nodeID)
	fmt.Printf("NUM_NODES:                   %d\n", s.numNodes)
	fmt.Printf("HOST_NODE_ADDRESS:           %s\n", s.hostNodeAddress)
	fmt.Printf("CLIENT_NODE_ADDRESSES:       %v\n", s.clientNodeAddress)
	fmt.Printf("NUM_PEER_CONNECTIONS:        %d\n", s.numPeerConnections)
	fmt.Printf("PEER_NODE_ADDRESSES:         %v\n", s.peerNodeAddresses)

	return s
}

func (s *RaftServer) setupRaftConfig() {
	s.config = &raft.Config{
		ID:              uint64(s.nodeID),
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         s.storage,
		MaxSizePerMsg:   math.MaxUint64,
		MaxInflightMsgs: 5000000,
	}

	peers := make([]raft.Peer, s.numNodes)
	for i := 0; i < s.numNodes; i++ {
		peers[i].ID = uint64(i + 1)
	}
	s.node = raft.StartNode(s.config, peers)
}

func (s *RaftServer) initializePeerConnections() {
	s.peerConnections = make([][]*shared.Client, s.numNodes)
	for i := range s.peerConnections {
		s.peerConnections[i] = make([]*shared.Client, 0)
	}

	for i, nodeAddress := range s.peerNodeAddresses {
		if nodeAddress == s.hostNodeAddress {
			continue
		}
		s.connectToPeer(nodeAddress, i)
	}

	fmt.Printf("Made all peer connections!\n")
}

func (s *RaftServer) connectToPeer(address string, index int) {
	for {
		conn, err := net.Dial("tcp", address)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		client := &shared.Client{Connection: conn, Mutex: &sync.Mutex{}}
		s.peerConnections[index] = append(s.peerConnections[index], client)
		if len(s.peerConnections[index]) != s.numPeerConnections {
			continue
		}
		break
	}
}

func (s *RaftServer) startNetworkListeners() {
	s.waitGroup.Add((s.numNodes * s.numPeerConnections) - s.numPeerConnections)

	peerListener := s.createListener(s.hostNodeAddress)
	clientListener := s.createListener(s.clientNodeAddress)

	go s.handleClientConnections(clientListener)
	go s.handlePeerConnections(peerListener)
}

func (s *RaftServer) createListener(address string) net.Listener {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to listen on %s: %v", address, err)
	}
	return listener
}

func (s *RaftServer) handleClientConnections(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Client connection error: %v", err)
			continue
		}

		log.Println("Client connected")
		client := &shared.Client{Connection: conn, Mutex: &sync.Mutex{}}
		go s.processClientMessages(client)
	}
}

func (s *RaftServer) processClientMessages(client *shared.Client) {
	sizeBuffer := make([]byte, 4)
	readBuffer := make([]byte, s.config.MaxSizePerMsg)

	for {
		if err := client.Read(sizeBuffer); err != nil {
			log.Printf("Error reading size: %v", err)
			return
		}

		amount := binary.LittleEndian.Uint32(sizeBuffer)
		if err := client.Read(readBuffer[:amount]); err != nil {
			log.Printf("Error reading message: %v", err)
			return
		}

		if s.node.Status().Lead == s.config.ID {
			messageId := binary.LittleEndian.Uint32(readBuffer[:4])
			s.senders.Store(messageId, client)
			bufferCopy := make([]byte, amount)
			copy(bufferCopy, readBuffer[:amount])
			if err := s.node.Propose(context.TODO(), bufferCopy); err != nil {
				log.Printf("Proposal error: %v", err)
			}
		} else {
			log.Println("Non-leader received client message")
		}
	}
}

func (s *RaftServer) handlePeerConnections(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Peer connection error: %v", err)
			continue
		}

		client := &shared.Client{Connection: conn, Mutex: &sync.Mutex{}}
		go s.processPeerMessages(client)
	}
}

func (s *RaftServer) processPeerMessages(client *shared.Client) {
	s.waitGroup.Done()
	sizeBuffer := make([]byte, 4)
	//TODO: figure out why bulking means this cant just be s.config.MaxSizePerMsg size
	readBuffer := make([]byte, 10000000)

	for {
		if err := client.Read(sizeBuffer); err != nil {
			log.Printf("Error reading size: %v", err)
			return
		}

		//fmt.Println("Got a peer message?")

		amount := binary.LittleEndian.Uint32(sizeBuffer)
		if err := client.Read(readBuffer[:amount]); err != nil {
			log.Printf("Error reading message: %v", err)
			return
		}

		var msg raftpb.Message
		if err := msg.Unmarshal(readBuffer[:amount]); err != nil {
			log.Printf("Unmarshal error: %v", err)
			continue
		}

		if err := s.node.Step(context.TODO(), msg); err != nil {
			log.Printf("Step error: %v", err)
		}
	}
}

func (s *RaftServer) runRaftLoop() {
	s.waitGroup.Wait()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	fmt.Println("Gets here?")
	for {
		select {
		case <-ticker.C:
			s.node.Tick()
		case rd := <-s.node.Ready():
			s.handleReady(rd)
			s.node.Advance()
		}
	}
}

func (s *RaftServer) handleReady(rd raft.Ready) {
	if len(rd.Entries) > 0 {
		s.appendEntries(rd.Entries)
	}
	//
	//s.sendMessages(rd.Messages)
	//
	//if !raft.IsEmptySnap(rd.Snapshot) {
	//	s.applySnapshot(rd.Snapshot)
	//}

	s.processCommittedEntries(rd.CommittedEntries)
}

func (s *RaftServer) appendEntries(entries []raftpb.Entry) {
	if err := s.storage.Append(entries); err != nil {
		log.Printf("Append entries error: %v", err)
	}
}

var sizeBuffer = make([]byte, 4)

func (s *RaftServer) sendMessages(messages []raftpb.Message) {
	//for _, msg := range messages {
	//	bytes, err := msg.Marshal()
	//	if err != nil {
	//		log.Printf("Marshal error: %v", err)
	//		continue
	//	}
	//
	//	//fmt.Printf("Sending %d bytes\n", len(bytes))
	//	binary.LittleEndian.PutUint32(sizeBuffer, uint32(len(bytes)))
	//
	//	connection := s.peerConnections[msg.To-1][0]
	//	connection.Mutex.Lock()
	//	if err := connection.Write(sizeBuffer); err != nil {
	//		connection.Mutex.Unlock()
	//		log.Printf("Write error: %v", err)
	//		continue
	//	}
	//	if err := connection.Write(bytes); err != nil {
	//		connection.Mutex.Unlock()
	//		log.Printf("Write error: %v", err)
	//		continue
	//	}
	//	connection.Mutex.Unlock()
	//}
}

func (s *RaftServer) applySnapshot(snapshot raftpb.Snapshot) {
	fmt.Println("Applying snapshot...")
}

// TODO: find out why if I propose from within the node I can get 150k ops
// but if messages come from the client we linger around 50-70k
var MESSAGES = int32(5000000)
var COMMITTED = int32(0)
var START = int64(0)
var DATA_SIZE = int(10000000)

func (s *RaftServer) processCommittedEntries(entries []raftpb.Entry) {
	for _, entry := range entries {
		switch entry.Type {
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(entry.Data); err != nil {
				log.Printf("Unmarshal conf change error: %v", err)
				continue
			}
			s.node.ApplyConfChange(cc)
		case raftpb.EntryNormal:
			// Apply to state machine
			//fmt.Printf("Commit proposal of size %d\n", len(entry.Data))
			//fmt.Printf("Committing something %d, %d, %d\n", s.node.Status().Lead, s.config.ID, len(entry.Data))
			if s.node.Status().Lead == s.config.ID && len(entry.Data) >= 4 {
				messageId := binary.LittleEndian.Uint32(entry.Data[:4])
				//fmt.Printf("Committing %d\n", messageId)
				//TODO: determine whether this is correct solution
				//COMMITTED += 1
				//if COMMITTED%100000 == 0 {
				//	fmt.Printf("Committing %d, %d == %d\n", COMMITTED, MESSAGES)
				//}
				//if COMMITTED == MESSAGES {
				//	seconds := float64(time.Now().UnixMilli()-START) / 1000.0
				//	fmt.Printf("%f OPS", float64(MESSAGES)/seconds)
				//}

				senderAny, ok := s.senders.LoadAndDelete(messageId)
				if ok {
					sender := senderAny.(*shared.Client)
					sender.Mutex.Lock()
					if err := sender.Write(entry.Data[:4]); err != nil {
						sender.Mutex.Unlock()
						log.Printf("Write error: %v", err)
						continue
					} else {
						sender.Mutex.Unlock()
					}
					fmt.Printf("Committing %d\n", messageId)
				}
			}
		}
	}
}

func Server() {
	server := NewRaftServer()
	server.setupRaftConfig()
	server.startNetworkListeners()
	server.initializePeerConnections()
	server.runRaftLoop()
}

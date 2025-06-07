package server

import (
	"math"
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
	poolWarmupSize           int
	hostNodeAddress          string
	clientNodeAddress        string
	pool                     *sync.Pool
	peerNodeAddresses        []string
	peerConnections          [][]*net.Conn
	senders                  sync.Map
	node                     raft.Node
	storage                  *raft.MemoryStorage
	config                   *raft.Config
	waitGroup                sync.WaitGroup
}

const (
	WarmupMsgMarker = 0xFFFF0000 // Special marker for warmup messages
	NormalMsgMarker = 0x00000000 // Normal client messages
)

type ClientOp struct {
	connection *net.Conn
	channel    *chan []byte
	data       *[]byte
}

func NewRaftServer() *RaftServer {

	//f, err := os.Create("cpu.prof")
	//if err != nil {
	//	log.Fatal(err)
	//}
	//defer f.Close()
	//
	//if err := pprof.StartCPUProfile(f); err != nil {
	//	log.Fatal(err)
	//}

	// Stop profiling after 30 seconds

	peerAddresses := strings.Split(os.Getenv("PEER_NODE_ADDRESSES"), ",")
	dataSize := shared.GetEnvInt("DATA_SIZE", 1)
	poolWarmupSize := shared.GetEnvInt("POOL_WARMUP_SIZE", 1)

	var pool = sync.Pool{
		New: func() interface{} {
			return make([]byte, dataSize)
		},
	}

	for range poolWarmupSize {
		pool.Put(pool.Get())
	}

	s := &RaftServer{
		numNodes:           len(peerAddresses),
		nodeID:             shared.GetEnvInt("NODE_ID", 1),
		hostNodeAddress:    os.Getenv("HOST_NODE_ADDRESS"),
		clientNodeAddress:  os.Getenv("CLIENT_NODE_ADDRESS"),
		peerNodeAddresses:  peerAddresses,
		numPeerConnections: shared.GetEnvInt("NUM_PEER_CONNECTIONS", 1),
		dataSize:           dataSize,
		poolWarmupSize:     poolWarmupSize,
		pool:               &pool,
		storage:            raft.NewMemoryStorage(),
	}

	start := time.Now()
	fmt.Printf("Warmup took %v\n", time.Since(start))

	//go func() {
	//	log.Println(http.ListenAndServe("0.0.0.0:6060", nil))
	//}()

	fmt.Println("=== Server Configuration ===")
	fmt.Printf("NODE_ID:                     %d\n", s.nodeID)
	fmt.Printf("NUM_NODES:                   %d\n", s.numNodes)
	fmt.Printf("HOST_NODE_ADDRESS:           %s\n", s.hostNodeAddress)
	fmt.Printf("CLIENT_NODE_ADDRESSES:       %v\n", s.clientNodeAddress)
	fmt.Printf("NUM_PEER_CONNECTIONS:        %d\n", s.numPeerConnections)
	fmt.Printf("PEER_NODE_ADDRESSES:         %v\n", s.peerNodeAddresses)

	return s
}

func (s *RaftServer) warmup() {
	warmupData := make([]byte, 8)
	binary.LittleEndian.PutUint32(warmupData[0:4], WarmupMsgMarker)

	for i := 0; i < 1000000; i++ { // Send enough to warm up all components
		binary.LittleEndian.PutUint32(warmupData[4:8], uint32(i)) // Sequence number
		if err := s.node.Propose(context.TODO(), warmupData); err != nil {
			log.Printf("Warmup proposal failed: %v", err)
		}
	}
}

func (s *RaftServer) setupRaftConfig() {
	s.config = &raft.Config{
		ID:                        uint64(s.nodeID),
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   s.storage,
		MaxSizePerMsg:             math.MaxUint32,
		MaxInflightMsgs:           5000000,
		MaxUncommittedEntriesSize: 0,
		MaxCommittedSizePerReady:  1000000,
	}

	peers := make([]raft.Peer, s.numNodes)
	for i := 0; i < s.numNodes; i++ {
		peers[i].ID = uint64(i + 1)
	}
	s.node = raft.StartNode(s.config, peers)
}

func (s *RaftServer) initializePeerConnections() {
	//s.peerConnections = make([][]*shared.Client, s.numNodes)
	//for i := range s.peerConnections {
	//	s.peerConnections[i] = make([]*shared.Client, 0)
	//}
	//
	//for i, nodeAddress := range s.peerNodeAddresses {
	//	if nodeAddress == s.hostNodeAddress {
	//		continue
	//	}
	//	s.connectToPeer(nodeAddress, i)
	//}
	//
	//fmt.Printf("Made all peer connections!\n")
}

func (s *RaftServer) connectToPeer(address string, index int) {
	//for {
	//	conn, err := net.Dial("tcp", address)
	//	if err != nil {
	//		time.Sleep(1 * time.Second)
	//		continue
	//	}
	//	err = conn.(*net.TCPConn).SetNoDelay(true)
	//	if err != nil {
	//		panic("error setting no delay")
	//	}
	//	client := &shared.Client{Connection: conn, Mutex: &sync.Mutex{}}
	//	s.peerConnections[index] = append(s.peerConnections[index], client)
	//	if len(s.peerConnections[index]) != s.numPeerConnections {
	//		continue
	//	}
	//	break
	//}
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
		writeChan := make(chan []byte, 1000000)
		go s.handleClientWrites(conn, writeChan)
		go s.processClientMessages(&writeChan, conn)
	}
}

func (s *RaftServer) handleClientWrites(conn net.Conn, writeChan chan []byte) {
	defer func() {
		err := conn.Close()
		if err != nil {
			panic(err)
		}
		close(writeChan)
	}()

	for msg := range writeChan {
		err := shared.Write(conn, msg[:4])
		//length := len(msg)
		//id := binary.LittleEndian.Uint32(msg[:4])
		if err != nil {
			panic(err)
		}
		s.pool.Put(msg)
	}
}

func (s *RaftServer) processClientMessages(writeChannel *chan []byte, conn net.Conn) {
	readBuffer := make([]byte, s.dataSize+4)
	proposalChannel := make(chan []byte, 1000000)

	go func() {
		for data := range proposalChannel {
			if err := s.node.Propose(context.TODO(), data); err != nil {
				log.Printf("Proposal error: %v", err)
			}
		}

		defer func() {
			close(proposalChannel)
			conn.Close()
		}()
	}()

	for {
		if err := shared.Read(conn, readBuffer[:4]); err != nil {
			log.Printf("Error reading size: %v", err)
			return
		}

		amount := binary.LittleEndian.Uint32(readBuffer[:4])
		//fmt.Printf("Got amount: %d\n", amount)
		if err := shared.Read(conn, readBuffer[:amount]); err != nil {
			log.Printf("Error reading message: %v", err)
			return
		}

		if s.node.Status().Lead == s.config.ID {
			messageId := binary.LittleEndian.Uint32(readBuffer[:4])
			bufferCopy := s.pool.Get().([]byte)
			copy(bufferCopy, readBuffer[:amount])
			//log.Printf("stroing messageid: %d\n", messageId)
			s.senders.Store(messageId, &ClientOp{
				connection: &conn,
				channel:    writeChannel,
				data:       &bufferCopy,
			})
			//
			//senderAny, ok := s.senders.LoadAndDelete(messageId)
			//if ok {
			//	sender := senderAny.(*ClientOp)
			//	channel := *sender.channel
			//	select {
			//	case channel <- *sender.data:
			//	default:
			//		log.Printf("Client write channel is full, dropping message?")
			//	}
			//	//fmt.Printf("Committing %d\n", messageId)
			//} else {
			//	log.Panicf("PROBLEM LOADING SENDER FOR %d", messageId)
			//}

			select {
			case proposalChannel <- bufferCopy[:amount]:
			default:
				log.Printf("Proposal channel is full, dropping message?")
			}
		} else {
			log.Println("Non-leader received client message")
		}
	}
}

func (s *RaftServer) runRaftLoop() {
	s.waitGroup.Wait()
	ticker := time.NewTicker(100 * time.Millisecond)
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

	if !raft.IsEmptySnap(rd.Snapshot) {
		s.applySnapshot(rd.Snapshot)
	}

	s.processCommittedEntries(rd.CommittedEntries)
}

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
			if len(entry.Data) >= 4 && s.node.Status().Lead == s.config.ID {
				messageId := binary.LittleEndian.Uint32(entry.Data[:4])
				//log.Printf("loading messageid: %d\n", messageId)
				senderAny, ok := s.senders.LoadAndDelete(messageId)
				if ok {
					sender := senderAny.(*ClientOp)
					channel := *sender.channel
					select {
					case channel <- *sender.data:
					default:
						log.Printf("Client write channel is full, dropping message?")
					}
					//fmt.Printf("Committing %d\n", messageId)
				} else {
					log.Panicf("PROBLEM LOADING SENDER FOR %d", messageId)
				}
			}
		}
	}
}

func (s *RaftServer) appendEntries(entries []raftpb.Entry) {
	if err := s.storage.Append(entries); err != nil {
		log.Printf("Append entries error: %v", err)
	}
}

var sizeBuffer = make([]byte, 4)

func (s *RaftServer) sendMessages(messages []raftpb.Message) {
	for _, msg := range messages {
		bytes, err := msg.Marshal()
		if err != nil {
			log.Printf("Marshal error: %v", err)
			continue
		}

		//fmt.Printf("Sending %d bytes\n", len(bytes))
		binary.LittleEndian.PutUint32(sizeBuffer, uint32(len(bytes)))
		//
		//connection := s.peerConnections[msg.To-1][0]
		//connection.Mutex.Lock()
		//if err := connection.Write(sizeBuffer); err != nil {
		//	connection.Mutex.Unlock()
		//	log.Printf("Write error: %v", err)
		//	continue
		//}
		//if err := connection.Write(bytes); err != nil {
		//	connection.Mutex.Unlock()
		//	log.Printf("Write error: %v", err)
		//	continue
		//}
		//connection.Mutex.Unlock()
	}
}

func (s *RaftServer) applySnapshot(snapshot raftpb.Snapshot) {
	fmt.Println("Applying snapshot...")
}

func (s *RaftServer) handlePeerConnections(listener net.Listener) {
	//for {
	//	conn, err := listener.Accept()
	//	if err != nil {
	//		log.Printf("Peer connection error: %v", err)
	//		continue
	//	}
	//
	//	client := &shared.Client{Connection: conn, Mutex: &sync.Mutex{}}
	//	go s.processPeerMessages(client)
	//}
}

func (s *RaftServer) processPeerMessages(client *net.Conn) {
	s.waitGroup.Done()
	//sizeBuffer := make([]byte, 4)
	////TODO: figure out why bulking means this cant just be s.config.MaxSizePerMsg size
	//readBuffer := make([]byte, 10000000)
	//
	//for {
	//	if err := client.Read(sizeBuffer); err != nil {
	//		log.Printf("Error reading size: %v", err)
	//		return
	//	}
	//
	//	//fmt.Println("Got a peer message?")
	//
	//	amount := binary.LittleEndian.Uint32(sizeBuffer)
	//	if err := client.Read(readBuffer[:amount]); err != nil {
	//		log.Printf("Error reading message: %v", err)
	//		return
	//	}
	//
	//	var msg raftpb.Message
	//	if err := msg.Unmarshal(readBuffer[:amount]); err != nil {
	//		log.Printf("Unmarshal error: %v", err)
	//		continue
	//	}
	//
	//	if err := s.node.Step(context.TODO(), msg); err != nil {
	//		log.Printf("Step error: %v", err)
	//	}
	//}
}

func Server() {
	server := NewRaftServer()
	server.setupRaftConfig()
	server.startNetworkListeners()
	server.initializePeerConnections()

	server.runRaftLoop()
}

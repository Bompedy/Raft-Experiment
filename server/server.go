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
	peerConnections          [][]net.Conn
	stepChannels             [][]chan raftpb.Message
	writeChannels            [][]chan ClientWrite
	senders                  sync.Map
	times                    sync.Map
	node                     raft.Node
	storage                  *raft.MemoryStorage
	config                   *raft.Config
	listenerGroup            sync.WaitGroup
	connectGroup             sync.WaitGroup
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

type ClientWrite struct {
	buffer *[]byte
	size   int
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

	//TODO: I allocate more here because we need it for marshalling messages, maybe want a new pool
	var pool = sync.Pool{
		New: func() interface{} {
			return make([]byte, dataSize+1024)
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
	for range 10 {
		go func() {
			for i := 0; i < 500000; i++ {
				binary.LittleEndian.PutUint32(warmupData, uint32(i))
				if err := s.node.Propose(context.TODO(), warmupData); err != nil {
					log.Printf("Warmup proposal failed: %v", err)
				}
			}
		}()
	}
}

func (s *RaftServer) setupRaftConfig() {
	s.config = &raft.Config{
		ID:                        uint64(s.nodeID),
		ElectionTick:              20,
		HeartbeatTick:             1,
		Storage:                   s.storage,
		MaxSizePerMsg:             math.MaxUint32,
		MaxInflightMsgs:           1000000,
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
	s.peerConnections = make([][]net.Conn, s.numNodes)
	for i := range s.peerConnections {
		s.peerConnections[i] = make([]net.Conn, 0)
	}
	s.stepChannels = make([][]chan raftpb.Message, s.numNodes)
	for i := range s.stepChannels {
		s.stepChannels[i] = make([]chan raftpb.Message, 0)
	}
	s.writeChannels = make([][]chan ClientWrite, s.numNodes)
	for i := range s.writeChannels {
		s.writeChannels[i] = make([]chan ClientWrite, 0)
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
		err = conn.(*net.TCPConn).SetNoDelay(true)
		if err != nil {
			panic("error setting no delay")
		}
		s.peerConnections[index] = append(s.peerConnections[index], conn)
		s.stepChannels[index] = append(s.stepChannels[index], make(chan raftpb.Message, 100000))
		s.writeChannels[index] = append(s.writeChannels[index], make(chan ClientWrite, 100000))
		fmt.Printf("connectToPeer %d, %d\n", index, len(s.stepChannels[index]))
		s.connectGroup.Done()
		if len(s.peerConnections[index]) != s.numPeerConnections {
			continue
		}
		break
	}
}

func (s *RaftServer) startNetworkListeners() {
	s.listenerGroup.Add((s.numNodes * s.numPeerConnections) - s.numPeerConnections)
	s.connectGroup.Add((s.numNodes * s.numPeerConnections) - s.numPeerConnections)

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

		tcpConn := conn.(*net.TCPConn)
		err = tcpConn.SetNoDelay(true)
		if err != nil {
			return
		}

		log.Println("Client connected")
		writeChan := make(chan []byte, 100000)
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
		if err != nil {
			panic(err)
		}
		s.pool.Put(msg)
	}
}

func (s *RaftServer) processClientMessages(writeChannel *chan []byte, conn net.Conn) {
	readBuffer := make([]byte, s.dataSize+4)
	proposalChannel := make(chan []byte, 3000000)

	go func() {
		defer func() {
			close(proposalChannel)
			conn.Close()
		}()

		fmt.Printf("Setup proposal channel")

		for data := range proposalChannel {
			if err := s.node.Propose(context.TODO(), data); err != nil {
				log.Printf("Proposal error: %v", err)
			}
		}
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

			//s.times.Store(messageId, time.Now().UnixNano())

			bufferCopy := s.pool.Get().([]byte)
			copy(bufferCopy, readBuffer[:amount])
			//log.Printf("stroing messageid: %d\n", messageId)
			s.senders.Store(messageId, &ClientOp{
				connection: &conn,
				channel:    writeChannel,
				data:       &bufferCopy,
			})
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
	s.listenerGroup.Wait()
	s.connectGroup.Wait()

	go s.handlePeerStep()
	go s.handlePeerWrite()

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
	s.sendPeerMessages(rd.Messages)

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

				//if COMMITTED == 0 {
				//	fmt.Printf("Starting benchmark\n")
				//	START = time.Now()
				//}
				//
				//COMMITTED += 1
				//if COMMITTED%10000 == 0 {
				//	fmt.Printf("Index %d\n", COMMITTED)
				//}
				//if COMMITTED == 5000000 {
				//	total := time.Since(START)
				//	ops := 5000000 / total.Seconds()
				//	fmt.Printf("Total OPS: %f\n", ops)
				//}

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

func (s *RaftServer) sendPeerMessages(messages []raftpb.Message) {
	for _, msg := range messages {
		buffer := s.pool.Get().([]byte)
		bytes, err := msg.MarshalTo(buffer[4:])
		if err != nil {
			log.Printf("Marshal error: %v", err)
			continue
		}
		//fmt.Printf("Send peer message: %d -> %d\n", msg.From, msg.To)
		binary.LittleEndian.PutUint32(buffer[:4], uint32(bytes))
		channel := s.writeChannels[msg.To-1][msg.Index%uint64(s.numPeerConnections)]
		channel <- ClientWrite{
			&buffer,
			bytes + 4,
		}
	}
}

func (s *RaftServer) applySnapshot(snapshot raftpb.Snapshot) {
	fmt.Println("Applying snapshot...")
}

func (s *RaftServer) handlePeerConnections(listener net.Listener) {
	for {
		connection, err := listener.Accept()
		if err != nil {
			log.Printf("Peer connection error: %v", err)
			continue
		}
		go s.processPeerMessages(&connection)
	}
}

func (s *RaftServer) handlePeerStep() {
	for n := range s.numNodes {
		if n+1 == s.nodeID {
			continue
		}
		for c := range s.numPeerConnections {
			go func(nodeIndex int, connectionIndex int) {
				for {
					message := <-s.stepChannels[nodeIndex][connectionIndex]
					//fmt.Printf("Step %s, %d, %d\n", message.Type.String(), nodeIndex, len(s.stepChannels[nodeIndex]))
					if err := s.node.Step(context.TODO(), message); err != nil {
						log.Printf("Proposal error: %v", err)
					}
				}
			}(n, c)
		}
	}
}

func (s *RaftServer) handlePeerWrite() {
	for n := range s.numNodes {
		if n+1 == s.nodeID {
			continue
		}
		for c := range s.numPeerConnections {
			go func(nodeIndex int, connectionIndex int) {
				for {
					//fmt.Printf("handlePeerWrite %d, %d, %d\n", nodeIndex, len(s.peerConnections[nodeIndex]), len(s.writeChannels[nodeIndex]))
					connection := s.peerConnections[nodeIndex][connectionIndex]
					message := <-s.writeChannels[nodeIndex][connectionIndex]
					err := shared.Write(connection, (*message.buffer)[:message.size])
					if err != nil {
						panic(err)
					}
					s.pool.Put(*message.buffer)
				}
			}(n, c)
		}
	}
}

func (s *RaftServer) processPeerMessages(connection *net.Conn) {
	s.listenerGroup.Done()

	//sizeBuffer := make([]byte, 4)
	////TODO: figure out why bulking means this cant just be s.config.MaxSizePerMsg size
	buffer := make([]byte, 1024)

	for {
		err := shared.Read(*connection, buffer[:4])
		if err != nil {
			panic(err)
		}
		size := binary.LittleEndian.Uint32(buffer[:4])
		//fmt.Printf("size: %d\n", size)
		err = shared.Read(*connection, buffer[:size])
		if err != nil {
			panic(err)
		}
		var msg raftpb.Message
		if err := msg.Unmarshal(buffer[:size]); err != nil {
			log.Printf("Unmarshal error: %v", err)
			continue
		}
		if msg.From > 0 {
			//fmt.Printf("Peer message %d -> %d, %d\n", msg.From, msg.To, msg.Index%uint64(s.numPeerConnections))
			s.stepChannels[msg.From-1][msg.Index%uint64(s.numPeerConnections)] <- msg
		}
	}

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
	//go func() {
	//	time.Sleep(5 * time.Second)
	//	server.warmup()
	//}()
	server.runRaftLoop()
}

package main

import (
	"context"
	"fmt"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"log"
	"time"
)

func main() {
	// accept from client
	// client reader calls Propose(), write to log here
	// sends to Raft.Ready
	// Ready sends entries out to peers
	// peers call node.Step(entry)

	store := raft.NewMemoryStorage()
	config := &raft.Config{
		ID:              1,
		ElectionTick:    2,
		HeartbeatTick:   1,
		Storage:         store,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}
	node := raft.StartNode(config, []raft.Peer{{ID: 1}})
	ticker := time.NewTicker(10 * time.Millisecond)
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
			for _, entry := range rd.CommittedEntries {
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
			if len(rd.Entries) > 0 {
				log.Println("Appending entries:", rd.Entries)
				err := store.Append(rd.Entries)
				if err != nil {
					panic(err)
				}
			}
			for _, msg := range rd.Messages {
				log.Println("Sending message:", msg)
				//for i := range msg.Entries {
				//
				//}
				// In a real system, send this message to the target peer.
			}
			if len(rd.Messages) > 0 {
				log.Println("Messages:", rd.Messages)
			}
			node.Advance()
		}
	}
	//<-make(chan struct{})
}

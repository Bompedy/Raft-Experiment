package client

import (
	"fmt"
	"os"
	"raftlib/shared"
	"strings"
)

func Client() {
	numThreads := shared.GetEnvInt("NUM_NODES", 1)
	numNodes := shared.GetEnvInt("NUM_NODES", 1)
	numClients := shared.GetEnvInt("NUM_CLIENTS", 1)
	dataSize := shared.GetEnvInt("DATA_SIZE", 1)
	numOps := shared.GetEnvInt("NUM_OPS", 1)
	peerNodeAddresses := strings.Split(os.Getenv("PEER_NODE_ADDRESSES"), ",")

	fmt.Println("=== Client Configuration ===")
	fmt.Printf("NUM_THREADS:              %d\n", numThreads)
	fmt.Printf("NUM_NODES:                %d\n", numNodes)
	fmt.Printf("NUM_CLIENTS:              %d\n", numClients)
	fmt.Printf("DATA_SIZE:                %d\n", dataSize)
	fmt.Printf("NUM_OPS:                  %d\n", numOps)
	fmt.Printf("PEER_NODE_ADDRESSES:      %v\n", peerNodeAddresses)
}

package main

import (
	"os"
	"raftlib/client"
	"raftlib/server"
)

func main() {
	if (len(os.Args) < 2) || (os.Args[1] != "server" && os.Args[1] != "client") {
		panic("Usage: ./raftlib server|client")
	} else if os.Args[1] == "server" {
		server.Server()
	} else if os.Args[1] == "client" {
		client.Client()
	}
}

#!/bin/bash

if [ "$#" -ne 7 ]; then
    echo "Usage: $0 <NODE_ID> <DATA_SIZE> <NUM_NODES> <NUM_PEER_CONNECTIONS> <HOST_NODE_ADDRESS> <CLIENT_NODE_ADDRESS> <PEER_NODE_ADDRESSES>"
    exit 1
fi

NODE_ID=$1
DATA_SIZE=$2
NUM_NODES=$3
NUM_PEER_CONNECTIONS=$4
HOST_NODE_ADDRESS=$5
CLIENT_NODE_ADDRESS=$6
PEER_NODE_ADDRESSES=$7

echo "Pulling latest changes from Git..."
git pull || { echo "Git pull failed"; exit 1; }

echo "Building raft-experiment..."
go build || { echo "Go build failed"; exit 1; }

export NODE_ID
export DATA_SIZE
export NUM_NODES
export NUM_PEER_CONNECTIONS
export HOST_NODE_ADDRESS
export CLIENT_NODE_ADDRESS
export PEER_NODE_ADDRESSES

./raftlib.exe server
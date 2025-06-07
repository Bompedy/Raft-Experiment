#!/bin/bash

if [ "$#" -ne 7 ]; then
    echo "Usage: $0 <NODE_ID> <DATA_SIZE> <NUM_PEER_CONNECTIONS> <HOST_NODE_ADDRESS> <CLIENT_NODE_ADDRESS> <PEER_NODE_ADDRESSES> <POOL_WARMUP_SIZE>"
    exit 1
fi

NODE_ID=$1
DATA_SIZE=$2
NUM_PEER_CONNECTIONS=$3
HOST_NODE_ADDRESS=$4
CLIENT_NODE_ADDRESS=$5
PEER_NODE_ADDRESSES=$6
POOL_WARMUP_SIZE=$7

cd /local/Raft-Experiment || { echo "Failed to cd to /local/Raft-Experiment"; exit 1; }

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
export POOL_WARMUP_SIZE

./raftlib server
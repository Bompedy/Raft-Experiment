#!/bin/bash

if [ "$#" -ne 6 ]; then
    echo "Usage: $0 <CLOSED_LOOP> <DATA_SIZE> <NUM_CLIENTS> <NUM_THREADS> <NUM_OPS> <LEADER_NODE_ADDRESS>"
    exit 1
fi

CLOSED_LOOP=$1
DATA_SIZE=$2
NUM_CLIENTS=$3
NUM_THREADS=$4
NUM_OPS=$5
LEADER_NODE_ADDRESS=$6

cd /local/raft-experiment || { echo "Failed to cd to /local/Raft-Experiment"; exit 1; }

echo "Pulling latest changes from Git..."
git pull || { echo "Git pull failed"; exit 1; }

echo "Building raft-experiment..."
go build || { echo "Go build failed"; exit 1; }

export CLOSED_LOOP
export DATA_SIZE
export NUM_CLIENTS
export NUM_THREADS
export NUM_OPS
export LEADER_NODE_ADDRESS

./raftlib.exe client
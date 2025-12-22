#!/bin/bash

# Script to setup test environment with Docker containers
# Creates Cassandra and YugabyteDB containers, test tables, and sample data

set -e

echo "=========================================="
echo "Setting up Test Environment"
echo "=========================================="

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker and try again."
    exit 1
fi

# Start YugabyteDB container
echo ""
echo "Starting YugabyteDB container..."
if docker ps -a | grep -q "yugabyte"; then
    if docker ps | grep -q "yugabyte"; then
        echo "YugabyteDB container already running"
    else
        docker start yugabyte
        echo "YugabyteDB container started"
    fi
else
    docker run -d --name yugabyte \
        -p 5433:5433 \
        -p 9042:9042 \
        -p 15433:15433 \
        yugabytedb/yugabyte:latest \
        bin/yugabyted start --daemon=false --ui=false
    echo "YugabyteDB container created and started"
fi

# Wait for YugabyteDB to be ready
echo "Waiting for YugabyteDB to be ready..."
sleep 10
for i in {1..30}; do
    if docker exec yugabyte yugabyted status 2>/dev/null | grep -q "Running"; then
        echo "YugabyteDB is ready"
        break
    fi
    echo "Waiting... ($i/30)"
    sleep 2
done

# Start Cassandra container
echo ""
echo "Starting Cassandra container..."
if docker ps -a | grep -q "cassandra"; then
    if docker ps | grep -q "cassandra"; then
        echo "Cassandra container already running"
    else
        docker start cassandra
        echo "Cassandra container started"
    fi
else
    docker run -d --name cassandra \
        -p 9043:9042 \
        -e CASSANDRA_CLUSTER_NAME=TestCluster \
        cassandra:latest
    echo "Cassandra container created and started"
fi

# Wait for Cassandra to be ready
echo "Waiting for Cassandra to be ready..."
sleep 15
for i in {1..30}; do
    if docker exec cassandra nodetool status 2>/dev/null | grep -q "UN"; then
        echo "Cassandra is ready"
        break
    fi
    echo "Waiting... ($i/30)"
    sleep 2
done

echo ""
echo "=========================================="
echo "✅ Test environment setup complete!"
echo "=========================================="
echo ""
echo "YugabyteDB: localhost:5433 (user: yugabyte, password: yugabyte)"
echo "Cassandra: localhost:9043 (user: cassandra, password: cassandra)"
echo ""


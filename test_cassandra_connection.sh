#!/bin/bash
# Test Cassandra connection from host

echo "=== Testing Cassandra Connection ==="
echo ""

# Test 1: Check if port 9043 is accessible
echo "1. Testing port 9043 accessibility..."
if nc -zv localhost 9043 2>&1 | grep -q "succeeded"; then
    echo "   ✅ Port 9043 is accessible"
else
    echo "   ❌ Port 9043 is NOT accessible"
fi

# Test 2: Check Docker container
echo ""
echo "2. Checking Docker container..."
if docker ps | grep -q cassandra; then
    echo "   ✅ Cassandra container is running"
    CONTAINER_ID=$(docker ps | grep cassandra | awk '{print $1}')
    echo "   Container ID: $CONTAINER_ID"
else
    echo "   ❌ Cassandra container is NOT running"
fi

# Test 3: Test connection from inside container
echo ""
echo "3. Testing connection from inside container (port 9042)..."
docker exec -i cassandra cqlsh localhost 9042 -e "SELECT keyspace_name FROM system_schema.keyspaces;" 2>&1 | grep -E "(transaction_datastore|test_keyspace|system)" | head -5

# Test 4: List keyspaces
echo ""
echo "4. Available keyspaces:"
docker exec -i cassandra cqlsh localhost 9042 -e "SELECT keyspace_name FROM system_schema.keyspaces;" 2>&1 | grep -v "^$" | grep -v "keyspace_name" | grep -v "---" | grep -v "^$" | head -10

echo ""
echo "=== Connection Test Complete ==="

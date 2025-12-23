#!/bin/bash
# Performance diagnostic script for remote migration

set -e

echo "=========================================="
echo "Performance Diagnostic Tool"
echo "=========================================="
echo ""

# Check if properties file is provided
PROPERTIES_FILE=${1:-"migration.properties"}
if [ ! -f "$PROPERTIES_FILE" ]; then
    echo "ERROR: Properties file not found: $PROPERTIES_FILE"
    exit 1
fi

echo "Using properties file: $PROPERTIES_FILE"
echo ""

# Extract connection details
CASSANDRA_HOST=$(grep "^cassandra.host=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1)
CASSANDRA_PORT=$(grep "^cassandra.port=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1)
YUGABYTE_HOST=$(grep "^yugabyte.host=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1 | cut -d',' -f1)
YUGABYTE_PORT=$(grep "^yugabyte.port=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1)

echo "=========================================="
echo "1. Network Latency Tests"
echo "=========================================="

# Test Cassandra latency
if [ -n "$CASSANDRA_HOST" ] && [ -n "$CASSANDRA_PORT" ]; then
    echo ""
    echo "Cassandra ($CASSANDRA_HOST:$CASSANDRA_PORT):"
    if command -v nc >/dev/null 2>&1; then
        echo "  Testing TCP connection..."
        time (echo > /dev/tcp/$CASSANDRA_HOST/$CASSANDRA_PORT) 2>&1 | grep real || echo "  Connection test failed"
    fi
    
    if command -v ping >/dev/null 2>&1; then
        echo "  Ping test (5 packets):"
        ping -c 5 "$CASSANDRA_HOST" 2>&1 | tail -3
    fi
fi

# Test YugabyteDB latency
if [ -n "$YUGABYTE_HOST" ] && [ -n "$YUGABYTE_PORT" ]; then
    echo ""
    echo "YugabyteDB ($YUGABYTE_HOST:$YUGABYTE_PORT):"
    if command -v nc >/dev/null 2>&1; then
        echo "  Testing TCP connection..."
        time (echo > /dev/tcp/$YUGABYTE_HOST/$YUGABYTE_PORT) 2>&1 | grep real || echo "  Connection test failed"
    fi
    
    if command -v ping >/dev/null 2>&1; then
        echo "  Ping test (5 packets):"
        ping -c 5 "$YUGABYTE_HOST" 2>&1 | tail -3
    fi
fi

echo ""
echo "=========================================="
echo "2. Configuration Analysis"
echo "=========================================="

echo ""
echo "Spark Configuration:"
echo "  Executor instances: $(grep "^spark.executor.instances=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1 || echo 'not set')"
echo "  Executor cores: $(grep "^spark.executor.cores=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1 || echo 'not set')"
echo "  Executor memory: $(grep "^spark.executor.memory=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1 || echo 'not set')"
echo "  Default parallelism: $(grep "^spark.default.parallelism=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1 || echo 'not set')"

echo ""
echo "Cassandra Configuration:"
echo "  Host: $CASSANDRA_HOST"
echo "  Port: $CASSANDRA_PORT"
echo "  Fetch size: $(grep "^cassandra.fetchSizeInRows=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1 || echo 'not set')"
echo "  Split size: $(grep "^cassandra.inputSplitSizeMb=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1 || echo 'not set') MB"
echo "  Concurrent reads: $(grep "^cassandra.concurrentReads=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1 || echo 'not set')"

echo ""
echo "YugabyteDB Configuration:"
echo "  Host: $YUGABYTE_HOST"
echo "  Port: $YUGABYTE_PORT"
echo "  Copy buffer size: $(grep "^yugabyte.copyBufferSize=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1 || echo 'not set')"
echo "  Copy flush every: $(grep "^yugabyte.copyFlushEvery=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1 || echo 'not set')"

echo ""
echo "=========================================="
echo "3. System Resources"
echo "=========================================="

echo ""
echo "CPU:"
if command -v nproc >/dev/null 2>&1; then
    echo "  Cores: $(nproc)"
fi
if [ -f /proc/cpuinfo ]; then
    echo "  CPU Model: $(grep 'model name' /proc/cpuinfo | head -1 | cut -d':' -f2 | xargs)"
fi

echo ""
echo "Memory:"
if command -v free >/dev/null 2>&1; then
    free -h | head -2
elif [ -f /proc/meminfo ]; then
    echo "  Total: $(grep MemTotal /proc/meminfo | awk '{print $2/1024/1024 " GB"}')"
    echo "  Available: $(grep MemAvailable /proc/meminfo | awk '{print $2/1024/1024 " GB"}')"
fi

echo ""
echo "Network:"
if command -v ifconfig >/dev/null 2>&1; then
    ifconfig | grep -E "^[a-z]|inet " | head -6
elif command -v ip >/dev/null 2>&1; then
    ip addr show | grep -E "^[0-9]|inet " | head -6
fi

echo ""
echo "=========================================="
echo "4. Recommended Optimizations"
echo "=========================================="

echo ""
echo "For Remote/Production Environment:"
echo ""
echo "1. Network Optimization:"
echo "   - Ensure low latency (<10ms) between app node and databases"
echo "   - Use dedicated network/VPN for migration traffic"
echo "   - Consider increasing network bandwidth"
echo ""
echo "2. Spark Configuration (for remote):"
echo "   spark.executor.instances=8-16 (increase for remote)"
echo "   spark.executor.cores=4-8"
echo "   spark.executor.memory=8g-16g"
echo "   spark.default.parallelism=32-64 (increase for remote)"
echo ""
echo "3. Cassandra Configuration:"
echo "   cassandra.fetchSizeInRows=5000-10000 (reduce for high latency)"
echo "   cassandra.inputSplitSizeMb=128-256 (smaller for remote)"
echo "   cassandra.concurrentReads=1024-2048"
echo ""
echo "4. YugabyteDB Configuration:"
echo "   yugabyte.copyBufferSize=50000-100000"
echo "   yugabyte.copyFlushEvery=25000-50000"
echo "   yugabyte.loadBalanceHosts=true (use all YugabyteDB nodes)"
echo ""
echo "5. Connection Settings:"
echo "   - Use multiple YugabyteDB nodes: yugabyte.host=node1,node2,node3"
echo "   - Increase timeouts for remote: cassandra.readTimeoutMs=180000"
echo "   - Use connection pooling efficiently"
echo ""

echo "=========================================="
echo "Diagnostic Complete"
echo "=========================================="


#!/bin/bash
# Targeted diagnostic for slow performance with 3 YugabyteDB nodes

set -e

echo "=========================================="
echo "Performance Bottleneck Diagnostic"
echo "=========================================="
echo ""

PROPERTIES_FILE=${1:-"migration.properties"}
if [ ! -f "$PROPERTIES_FILE" ]; then
    echo "ERROR: Properties file not found: $PROPERTIES_FILE"
    exit 1
fi

echo "Analyzing: $PROPERTIES_FILE"
echo ""

# Extract configuration
YUGABYTE_HOSTS=$(grep "^yugabyte.host=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1)
YUGABYTE_PORT=$(grep "^yugabyte.port=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1)
CASSANDRA_HOST=$(grep "^cassandra.host=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1 | cut -d',' -f1)
CASSANDRA_PORT=$(grep "^cassandra.port=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1)
PARALLELISM=$(grep "^spark.default.parallelism=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1)
EXECUTOR_INSTANCES=$(grep "^spark.executor.instances=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1)
FETCH_SIZE=$(grep "^cassandra.fetchSizeInRows=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1)
SPLIT_SIZE=$(grep "^cassandra.inputSplitSizeMb=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1)

echo "=========================================="
echo "1. Configuration Check"
echo "=========================================="

echo ""
echo "YugabyteDB Configuration:"
echo "  Hosts: $YUGABYTE_HOSTS"
if [ -n "$YUGABYTE_HOSTS" ]; then
    NODE_COUNT=$(echo "$YUGABYTE_HOSTS" | tr ',' '\n' | wc -l | tr -d ' ')
    echo "  Node count: $NODE_COUNT"
    if [ "$NODE_COUNT" -lt 3 ]; then
        echo "  ⚠️  WARNING: Less than 3 nodes configured"
    fi
fi
echo "  Load balance: $(grep "^yugabyte.loadBalanceHosts=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1 || echo 'not set')"
echo "  Copy buffer: $(grep "^yugabyte.copyBufferSize=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1 || echo 'not set')"
echo "  Copy flush: $(grep "^yugabyte.copyFlushEvery=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1 || echo 'not set')"

echo ""
echo "Spark Configuration:"
echo "  Parallelism: ${PARALLELISM:-'not set (default: cores)'}"
echo "  Executor instances: ${EXECUTOR_INSTANCES:-'not set'}"
echo "  Executor cores: $(grep "^spark.executor.cores=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1 || echo 'not set')"
echo "  Executor memory: $(grep "^spark.executor.memory=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1 || echo 'not set')"

if [ -n "$PARALLELISM" ] && [ "$PARALLELISM" -lt 32 ]; then
    echo "  ⚠️  WARNING: Parallelism < 32 may be too low for remote environment"
fi

echo ""
echo "Cassandra Configuration:"
echo "  Host: $CASSANDRA_HOST"
echo "  Fetch size: ${FETCH_SIZE:-'not set'}"
echo "  Split size: ${SPLIT_SIZE:-'not set'} MB"

if [ -n "$FETCH_SIZE" ] && [ "$FETCH_SIZE" -gt 10000 ]; then
    echo "  ⚠️  WARNING: Large fetch size ($FETCH_SIZE) may be slow with high latency"
fi

echo ""
echo "=========================================="
echo "2. Network Latency Test"
echo "=========================================="

# Test YugabyteDB nodes
if [ -n "$YUGABYTE_HOSTS" ]; then
    echo ""
    echo "YugabyteDB Nodes Latency:"
    IFS=',' read -ra NODES <<< "$YUGABYTE_HOSTS"
    for node in "${NODES[@]}"; do
        node=$(echo "$node" | xargs)  # trim whitespace
        if [ -n "$node" ]; then
            echo "  Testing $node..."
            if command -v ping >/dev/null 2>&1; then
                AVG_LATENCY=$(ping -c 5 -q "$node" 2>&1 | grep "avg" | awk -F'/' '{print $5}' | cut -d'.' -f1)
                if [ -n "$AVG_LATENCY" ]; then
                    if [ "$AVG_LATENCY" -gt 50 ]; then
                        echo "    ⚠️  High latency: ${AVG_LATENCY}ms (target: <10ms)"
                    elif [ "$AVG_LATENCY" -gt 20 ]; then
                        echo "    ⚠️  Medium latency: ${AVG_LATENCY}ms (target: <10ms)"
                    else
                        echo "    ✅ Latency: ${AVG_LATENCY}ms"
                    fi
                fi
            fi
        fi
    done
fi

# Test Cassandra
if [ -n "$CASSANDRA_HOST" ]; then
    echo ""
    echo "Cassandra Latency:"
    if command -v ping >/dev/null 2>&1; then
        AVG_LATENCY=$(ping -c 5 -q "$CASSANDRA_HOST" 2>&1 | grep "avg" | awk -F'/' '{print $5}' | cut -d'.' -f1)
        if [ -n "$AVG_LATENCY" ]; then
            if [ "$AVG_LATENCY" -gt 50 ]; then
                echo "  ⚠️  High latency: ${AVG_LATENCY}ms (target: <10ms)"
            elif [ "$AVG_LATENCY" -gt 20 ]; then
                echo "  ⚠️  Medium latency: ${AVG_LATENCY}ms (target: <10ms)"
            else
                echo "  ✅ Latency: ${AVG_LATENCY}ms"
            fi
        fi
    fi
fi

echo ""
echo "=========================================="
echo "3. Recommended Optimizations"
echo "=========================================="

echo ""
echo "Based on your configuration, here are the top optimizations:"
echo ""

# Check parallelism
if [ -z "$PARALLELISM" ] || [ "$PARALLELISM" -lt 32 ]; then
    echo "1. ⚠️  INCREASE PARALLELISM (HIGH PRIORITY)"
    echo "   Current: ${PARALLELISM:-'default (too low)'}"
    echo "   Recommended: spark.default.parallelism=32-64"
    echo "   Expected improvement: +30-50%"
    echo ""
fi

# Check fetch size
if [ -z "$FETCH_SIZE" ] || [ "$FETCH_SIZE" -gt 10000 ]; then
    echo "2. ⚠️  REDUCE FETCH SIZE (MEDIUM PRIORITY)"
    echo "   Current: ${FETCH_SIZE:-'not set'}"
    echo "   Recommended: cassandra.fetchSizeInRows=5000"
    echo "   Reason: Smaller batches = faster with high latency"
    echo ""
fi

# Check split size
if [ -z "$SPLIT_SIZE" ] || [ "$SPLIT_SIZE" -gt 256 ]; then
    echo "3. ⚠️  REDUCE SPLIT SIZE (MEDIUM PRIORITY)"
    echo "   Current: ${SPLIT_SIZE:-'not set'} MB"
    echo "   Recommended: cassandra.inputSplitSizeMb=128"
    echo "   Reason: Smaller splits = better load balancing"
    echo ""
fi

# Check executor instances
if [ -z "$EXECUTOR_INSTANCES" ] || [ "$EXECUTOR_INSTANCES" -lt 8 ]; then
    echo "4. ⚠️  INCREASE EXECUTOR INSTANCES (MEDIUM PRIORITY)"
    echo "   Current: ${EXECUTOR_INSTANCES:-'not set'}"
    echo "   Recommended: spark.executor.instances=8-16"
    echo "   Expected improvement: +20-30%"
    echo ""
fi

# Check copy buffer
COPY_BUFFER=$(grep "^yugabyte.copyBufferSize=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1)
if [ -z "$COPY_BUFFER" ] || [ "$COPY_BUFFER" -gt 100000 ]; then
    echo "5. ⚠️  OPTIMIZE COPY BUFFER (LOW PRIORITY)"
    echo "   Current: ${COPY_BUFFER:-'not set'}"
    echo "   Recommended: yugabyte.copyBufferSize=50000"
    echo "   Reason: Balance between memory and network efficiency"
    echo ""
fi

echo "=========================================="
echo "4. Quick Fix Configuration"
echo "=========================================="

echo ""
echo "Add these to your properties file for immediate improvement:"
echo ""
echo "# Spark - Increase parallelism"
echo "spark.default.parallelism=32"
echo "spark.sql.shuffle.partitions=32"
echo "spark.executor.instances=8"
echo ""
echo "# Cassandra - Optimize for latency"
echo "cassandra.fetchSizeInRows=5000"
echo "cassandra.inputSplitSizeMb=128"
echo "cassandra.concurrentReads=2048"
echo ""
echo "# YugabyteDB - Verify load balancing"
echo "yugabyte.loadBalanceHosts=true"
echo "yugabyte.copyBufferSize=50000"
echo "yugabyte.copyFlushEvery=25000"
echo ""

echo "=========================================="
echo "Diagnostic Complete"
echo "=========================================="
echo ""
echo "Next Steps:"
echo "1. Apply recommended optimizations"
echo "2. Re-run migration and measure throughput"
echo "3. Monitor Spark UI for task execution times"
echo "4. Check network bandwidth usage during migration"
echo ""


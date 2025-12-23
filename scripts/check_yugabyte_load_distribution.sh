#!/bin/bash
# Check if load is distributed across all YugabyteDB nodes

set -e

echo "=========================================="
echo "YugabyteDB Load Distribution Check"
echo "=========================================="
echo ""

# Get connection details from properties file or environment
PROPERTIES_FILE=${1:-"migration.properties"}
if [ -f "$PROPERTIES_FILE" ]; then
    YUGABYTE_HOST=$(grep "^yugabyte.host=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1 | cut -d',' -f1)
    YUGABYTE_PORT=$(grep "^yugabyte.port=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1)
    YUGABYTE_DB=$(grep "^yugabyte.database=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1)
    YUGABYTE_USER=$(grep "^yugabyte.username=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1)
    YUGABYTE_PASS=$(grep "^yugabyte.password=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1)
else
    echo "Properties file not found. Using defaults or environment variables."
    YUGABYTE_HOST=${YUGABYTE_HOST:-"localhost"}
    YUGABYTE_PORT=${YUGABYTE_PORT:-"5433"}
    YUGABYTE_DB=${YUGABYTE_DB:-"yugabyte"}
    YUGABYTE_USER=${YUGABYTE_USER:-"yugabyte"}
    YUGABYTE_PASS=${YUGABYTE_PASS:-"yugabyte"}
fi

echo "Connecting to: $YUGABYTE_HOST:$YUGABYTE_PORT"
echo "Database: $YUGABYTE_DB"
echo ""

# Function to run SQL query
run_sql() {
    local sql="$1"
    local node="$2"
    local host="${node:-$YUGABYTE_HOST}"
    
    docker exec yugabyte bash -c "cd /home/yugabyte/bin && ./ysqlsh -h $host -U $YUGABYTE_USER -d $YUGABYTE_DB -t -c \"$sql\"" 2>&1 | grep -v "Warnings\|Note:" || \
    psql "postgresql://$YUGABYTE_USER:$YUGABYTE_PASS@$host:$YUGABYTE_PORT/$YUGABYTE_DB" -t -c "$sql" 2>&1 | grep -v "Warnings\|Note:" || \
    echo "  (Connection failed - check if node is accessible)"
}

echo "=========================================="
echo "1. Connection Distribution Across Nodes"
echo "=========================================="
echo ""

SQL_CONNECTIONS="
SELECT 
    CASE 
        WHEN host = '127.0.0.1' OR host LIKE '172.%' OR host LIKE '10.%' THEN 'Node (Internal IP)'
        ELSE host
    END as node,
    COUNT(*) as connections,
    COUNT(*) FILTER (WHERE state = 'active') as active,
    COUNT(*) FILTER (WHERE state = 'idle in transaction') as idle_in_transaction,
    COUNT(*) FILTER (WHERE query LIKE '%COPY%') as copy_operations
FROM pg_stat_activity
WHERE datname = '$YUGABYTE_DB'
  AND pid != pg_backend_pid()
GROUP BY host
ORDER BY connections DESC;
"

echo "Active connections per node:"
run_sql "$SQL_CONNECTIONS" "$YUGABYTE_HOST"

echo ""
echo "=========================================="
echo "2. COPY Operations Distribution"
echo "=========================================="
echo ""

SQL_COPY="
SELECT 
    CASE 
        WHEN host = '127.0.0.1' OR host LIKE '172.%' OR host LIKE '10.%' THEN 'Node (Internal IP)'
        ELSE host
    END as node,
    COUNT(*) as total_connections,
    COUNT(*) FILTER (WHERE query LIKE '%COPY%' OR query LIKE '%copy%') as copy_operations,
    COUNT(*) FILTER (WHERE state = 'active' AND (query LIKE '%COPY%' OR query LIKE '%copy%')) as active_copy
FROM pg_stat_activity
WHERE datname = '$YUGABYTE_DB'
  AND pid != pg_backend_pid()
GROUP BY host
ORDER BY copy_operations DESC;
"

echo "COPY operations per node:"
run_sql "$SQL_COPY" "$YUGABYTE_HOST"

echo ""
echo "=========================================="
echo "3. Query Activity Distribution"
echo "=========================================="
echo ""

SQL_QUERIES="
SELECT 
    CASE 
        WHEN host = '127.0.0.1' OR host LIKE '172.%' OR host LIKE '10.%' THEN 'Node (Internal IP)'
        ELSE host
    END as node,
    COUNT(*) as total_queries,
    COUNT(*) FILTER (WHERE state = 'active') as active_queries,
    ROUND(AVG(EXTRACT(EPOCH FROM (now() - query_start))), 2) as avg_query_duration_sec,
    MAX(EXTRACT(EPOCH FROM (now() - query_start))) as max_query_duration_sec
FROM pg_stat_activity
WHERE datname = '$YUGABYTE_DB'
  AND pid != pg_backend_pid()
GROUP BY host
ORDER BY active_queries DESC;
"

echo "Query activity per node:"
run_sql "$SQL_QUERIES" "$YUGABYTE_HOST"

echo ""
echo "=========================================="
echo "4. Transaction Distribution"
echo "=========================================="
echo ""

SQL_TXNS="
SELECT 
    CASE 
        WHEN host = '127.0.0.1' OR host LIKE '172.%' OR host LIKE '10.%' THEN 'Node (Internal IP)'
        ELSE host
    END as node,
    COUNT(*) FILTER (WHERE xact_start IS NOT NULL) as active_transactions,
    COUNT(*) FILTER (WHERE state = 'idle in transaction') as idle_transactions,
    COUNT(*) FILTER (WHERE state = 'active') as active_queries
FROM pg_stat_activity
WHERE datname = '$YUGABYTE_DB'
  AND pid != pg_backend_pid()
GROUP BY host
ORDER BY active_transactions DESC;
"

echo "Transaction distribution per node:"
run_sql "$SQL_TXNS" "$YUGABYTE_HOST"

echo ""
echo "=========================================="
echo "5. Load Balance Verification"
echo "=========================================="
echo ""

echo "Checking if connections are distributed:"
SQL_BALANCE="
SELECT 
    COUNT(DISTINCT host) as unique_nodes,
    COUNT(*) as total_connections,
    ROUND(COUNT(*)::numeric / COUNT(DISTINCT host), 2) as avg_connections_per_node,
    MIN(conn_count) as min_connections,
    MAX(conn_count) as max_connections,
    CASE 
        WHEN MAX(conn_count) - MIN(conn_count) <= 2 THEN '✅ Well balanced'
        WHEN MAX(conn_count) - MIN(conn_count) <= 5 THEN '⚠️  Some imbalance'
        ELSE '❌ Significant imbalance'
    END as balance_status
FROM (
    SELECT 
        host,
        COUNT(*) as conn_count
    FROM pg_stat_activity
    WHERE datname = '$YUGABYTE_DB'
      AND pid != pg_backend_pid()
    GROUP BY host
) subq;
"

run_sql "$SQL_BALANCE" "$YUGABYTE_HOST"

echo ""
echo "=========================================="
echo "6. Node-Specific Details"
echo "=========================================="
echo ""

SQL_NODE_DETAILS="
SELECT 
    host as node_ip,
    COUNT(*) as connections,
    string_agg(DISTINCT application_name, ', ') as applications,
    COUNT(*) FILTER (WHERE query LIKE '%COPY%') as copy_ops,
    COUNT(*) FILTER (WHERE state = 'active') as active
FROM pg_stat_activity
WHERE datname = '$YUGABYTE_DB'
  AND pid != pg_backend_pid()
GROUP BY host
ORDER BY connections DESC;
"

echo "Detailed connection info per node:"
run_sql "$SQL_NODE_DETAILS" "$YUGABYTE_HOST"

echo ""
echo "=========================================="
echo "Summary"
echo "=========================================="
echo ""
echo "✅ Load balancing is working if:"
echo "   1. Connections are distributed across all 3 nodes"
echo "   2. Each node has similar connection counts (within 2-3)"
echo "   3. COPY operations are present on multiple nodes"
echo "   4. Active queries are distributed"
echo ""
echo "❌ Load balancing issues if:"
echo "   1. All connections go to one node"
echo "   2. One node has significantly more connections"
echo "   3. COPY operations only on one node"
echo ""
echo "Note: If you see internal IPs (172.x, 10.x), those are the actual"
echo "      YugabyteDB node IPs. The driver distributes connections to these."
echo ""


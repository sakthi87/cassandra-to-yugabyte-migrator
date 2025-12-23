#!/bin/bash
# Check YugabyteDB load distribution - Fixed version using correct columns

set -e

echo "=========================================="
echo "YugabyteDB Load Distribution Check"
echo "=========================================="
echo ""

PROPERTIES_FILE=${1:-"migration.properties"}
if [ -f "$PROPERTIES_FILE" ]; then
    YUGABYTE_HOST=$(grep "^yugabyte.host=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1 | cut -d',' -f1)
    YUGABYTE_PORT=$(grep "^yugabyte.port=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1)
    YUGABYTE_DB=$(grep "^yugabyte.database=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1)
    YUGABYTE_USER=$(grep "^yugabyte.username=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1)
    YUGABYTE_PASS=$(grep "^yugabyte.password=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1)
else
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
    docker exec yugabyte bash -c "cd /home/yugabyte/bin && ./ysqlsh -h $YUGABYTE_HOST -U $YUGABYTE_USER -d $YUGABYTE_DB -t -c \"$sql\"" 2>&1 | grep -v "Warnings\|Note:" || \
    psql "postgresql://$YUGABYTE_USER:$YUGABYTE_PASS@$YUGABYTE_HOST:$YUGABYTE_PORT/$YUGABYTE_DB" -t -c "$sql" 2>&1 | grep -v "Warnings\|Note:" || \
    echo "  (Connection failed)"
}

echo "=========================================="
echo "1. Available Columns in pg_stat_activity"
echo "=========================================="
echo ""

SQL_COLUMNS="
SELECT column_name 
FROM information_schema.columns 
WHERE table_name = 'pg_stat_activity'
ORDER BY ordinal_position;
"

echo "Columns available:"
run_sql "$SQL_COLUMNS"

echo ""
echo "=========================================="
echo "2. Connection Distribution (Using client_addr)"
echo "=========================================="
echo ""

SQL_CONNECTIONS="
SELECT 
    COALESCE(client_addr::text, 'local') as client_address,
    COUNT(*) as connections,
    COUNT(*) FILTER (WHERE state = 'active') as active,
    COUNT(*) FILTER (WHERE state = 'idle in transaction') as idle_in_transaction,
    COUNT(*) FILTER (WHERE query LIKE '%COPY%' OR query LIKE '%copy%') as copy_operations
FROM pg_stat_activity
WHERE datname = '$YUGABYTE_DB'
  AND pid != pg_backend_pid()
GROUP BY client_addr
ORDER BY connections DESC;
"

echo "Connections by client address:"
run_sql "$SQL_CONNECTIONS"

echo ""
echo "=========================================="
echo "3. Current Node Information"
echo "=========================================="
echo ""

SQL_NODE="
SELECT 
    inet_server_addr() as server_ip,
    inet_server_port() as server_port,
    current_setting('listen_addresses') as listen_addresses;
"

echo "Current node info:"
run_sql "$SQL_NODE"

echo ""
echo "=========================================="
echo "4. YugabyteDB Server List"
echo "=========================================="
echo ""

SQL_SERVERS="
SELECT 
    host,
    port,
    is_primary,
    num_tablets
FROM yb_servers()
ORDER BY host;
"

echo "YugabyteDB cluster nodes:"
run_sql "$SQL_SERVERS"

echo ""
echo "=========================================="
echo "5. Connection Distribution Method"
echo "=========================================="
echo ""
echo "Since pg_stat_activity doesn't show which YugabyteDB node,"
echo "we need to query EACH node separately to see connections."
echo ""

# Get list of nodes from properties
if [ -f "$PROPERTIES_FILE" ]; then
    NODES=$(grep "^yugabyte.host=" "$PROPERTIES_FILE" | cut -d'=' -f2 | head -1)
    echo "Configured nodes: $NODES"
    echo ""
    echo "Querying each node separately:"
    echo ""
    
    IFS=',' read -ra NODE_ARRAY <<< "$NODES"
    for node in "${NODE_ARRAY[@]}"; do
        node=$(echo "$node" | xargs)  # trim whitespace
        echo "--- Node: $node ---"
        
        SQL_NODE_CONN="
        SELECT 
            COUNT(*) as total_connections,
            COUNT(*) FILTER (WHERE state = 'active') as active,
            COUNT(*) FILTER (WHERE query LIKE '%COPY%' OR query LIKE '%copy%') as copy_ops,
            COUNT(*) FILTER (WHERE datname = '$YUGABYTE_DB') as connections_to_${YUGABYTE_DB}
        FROM pg_stat_activity
        WHERE pid != pg_backend_pid();
        "
        
        docker exec yugabyte bash -c "cd /home/yugabyte/bin && ./ysqlsh -h $node -U $YUGABYTE_USER -d $YUGABYTE_DB -t -c \"$SQL_NODE_CONN\"" 2>&1 | grep -v "Warnings\|Note:" || \
        psql "postgresql://$YUGABYTE_USER:$YUGABYTE_PASS@$node:$YUGABYTE_PORT/$YUGABYTE_DB" -t -c "$SQL_NODE_CONN" 2>&1 | grep -v "Warnings\|Note:" || \
        echo "  (Could not connect to $node)"
        echo ""
    done
fi

echo ""
echo "=========================================="
echo "6. Alternative: Check via Application Name"
echo "=========================================="
echo ""

SQL_APP="
SELECT 
    application_name,
    COUNT(*) as connections,
    COUNT(*) FILTER (WHERE state = 'active') as active,
    COUNT(*) FILTER (WHERE query LIKE '%COPY%') as copy_ops
FROM pg_stat_activity
WHERE datname = '$YUGABYTE_DB'
  AND pid != pg_backend_pid()
GROUP BY application_name
ORDER BY connections DESC;
"

echo "Connections by application:"
run_sql "$SQL_APP"

echo ""
echo "=========================================="
echo "Summary"
echo "=========================================="
echo ""
echo "✅ To verify load balancing:"
echo "   1. Query each YugabyteDB node separately (section 5)"
echo "   2. Each node should have roughly equal connections"
echo "   3. COPY operations should be distributed"
echo ""
echo "⚠️  Note: pg_stat_activity shows connections TO the node,"
echo "   not which node the connection is FROM."
echo "   To see distribution, query each node individually."
echo ""


#!/bin/bash
# Simple one-liner to check connection distribution

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

echo "Connection Distribution Across YugabyteDB Nodes:"
echo ""

# Try docker first, then direct psql
docker exec yugabyte bash -c "cd /home/yugabyte/bin && ./ysqlsh -h $YUGABYTE_HOST -U $YUGABYTE_USER -d $YUGABYTE_DB -t -c \"
SELECT 
    host as node,
    COUNT(*) as connections,
    COUNT(*) FILTER (WHERE state = 'active') as active,
    COUNT(*) FILTER (WHERE query LIKE '%COPY%') as copy_ops
FROM pg_stat_activity
WHERE datname = '$YUGABYTE_DB'
  AND pid != pg_backend_pid()
GROUP BY host
ORDER BY connections DESC;
\"" 2>&1 | grep -v "Warnings\|Note:" || \
psql "postgresql://$YUGABYTE_USER:$YUGABYTE_PASS@$YUGABYTE_HOST:$YUGABYTE_PORT/$YUGABYTE_DB" -t -c "
SELECT 
    host as node,
    COUNT(*) as connections,
    COUNT(*) FILTER (WHERE state = 'active') as active,
    COUNT(*) FILTER (WHERE query LIKE '%COPY%') as copy_ops
FROM pg_stat_activity
WHERE datname = '$YUGABYTE_DB'
  AND pid != pg_backend_pid()
GROUP BY host
ORDER BY connections DESC;
" 2>&1 | grep -v "Warnings\|Note:"

echo ""
echo "Expected: Connections distributed across 3 nodes (roughly equal)"


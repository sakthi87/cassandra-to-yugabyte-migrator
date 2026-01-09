#!/bin/bash

# Test script for local migration testing
# Tests both COPY and INSERT modes

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo "=========================================="
echo "Local Migration Test"
echo "=========================================="
echo ""

# Check if Docker containers are running
echo "Checking Docker containers..."
if ! docker ps | grep -q yugabyte; then
    echo -e "${RED}ERROR: YugabyteDB container is not running${NC}"
    echo "Please start YugabyteDB container first"
    exit 1
fi

if ! docker ps | grep -q cassandra; then
    echo -e "${RED}ERROR: Cassandra container is not running${NC}"
    echo "Please start Cassandra container first"
    exit 1
fi

echo -e "${GREEN}✓${NC} Docker containers are running"
echo ""

# Check if JAR exists
JAR_FILE="target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar"
if [ ! -f "$JAR_FILE" ]; then
    echo -e "${YELLOW}JAR file not found. Building...${NC}"
    mvn package -DskipTests -q
    if [ $? -ne 0 ]; then
        echo -e "${RED}ERROR: Failed to build JAR${NC}"
        exit 1
    fi
fi

echo -e "${GREEN}✓${NC} JAR file found: $JAR_FILE"
echo ""

# Check if properties file exists
PROPERTIES_FILE="src/main/resources/migration.properties"
if [ ! -f "$PROPERTIES_FILE" ]; then
    echo -e "${RED}ERROR: Properties file not found: $PROPERTIES_FILE${NC}"
    exit 1
fi

echo -e "${GREEN}✓${NC} Properties file found"
echo ""

# Ask user which mode to test
echo "Which mode would you like to test?"
echo "1) COPY mode (default, faster)"
echo "2) INSERT mode (idempotent, handles duplicates)"
echo "3) Test both modes"
read -p "Enter choice [1-3] (default: 1): " choice
choice=${choice:-1}

TEST_MODE=""
case $choice in
    1)
        TEST_MODE="COPY"
        echo "Testing COPY mode..."
        ;;
    2)
        TEST_MODE="INSERT"
        echo "Testing INSERT mode..."
        ;;
    3)
        TEST_MODE="BOTH"
        echo "Testing both modes..."
        ;;
    *)
        echo -e "${RED}Invalid choice${NC}"
        exit 1
        ;;
esac

echo ""

# Function to run migration
run_migration() {
    local mode=$1
    local log_file="migration_test_${mode}_$(date +%Y%m%d_%H%M%S).log"
    
    echo "=========================================="
    echo "Running migration in ${mode} mode"
    echo "=========================================="
    echo "Log file: $log_file"
    echo ""
    
    # Create temporary properties file with specified mode
    local temp_props=$(mktemp)
    cp "$PROPERTIES_FILE" "$temp_props"
    
    # Update insertMode in temp properties
    if [ "$mode" == "INSERT" ]; then
        sed -i.bak 's/^yugabyte\.insertMode=.*/yugabyte.insertMode=INSERT/' "$temp_props"
        rm -f "${temp_props}.bak"
    else
        sed -i.bak 's/^yugabyte\.insertMode=.*/yugabyte.insertMode=COPY/' "$temp_props"
        rm -f "${temp_props}.bak"
    fi
    
    echo "Configuration:"
    grep "^yugabyte\.insertMode=" "$temp_props" || echo "yugabyte.insertMode=COPY (default)"
    grep "^yugabyte\.insertBatchSize=" "$temp_props" || echo "yugabyte.insertBatchSize=1000 (default)"
    echo ""
    
    # Run migration
    echo "Starting migration..."
    spark-submit \
        --master local[4] \
        --driver-memory 2G \
        --executor-memory 2G \
        --conf spark.default.parallelism=4 \
        --conf spark.sql.shuffle.partitions=4 \
        --properties-file "$temp_props" \
        --class com.company.migration.MainApp \
        "$JAR_FILE" 2>&1 | tee "$log_file"
    
    local exit_code=${PIPESTATUS[0]}
    rm -f "$temp_props"
    
    if [ $exit_code -eq 0 ]; then
        echo ""
        echo -e "${GREEN}✓ Migration completed successfully in ${mode} mode${NC}"
        echo ""
        
        # Check for partition logging
        if grep -q "Partition.*sample PKs" "$log_file"; then
            echo -e "${GREEN}✓ Partition logging is working${NC}"
            echo "Sample partition logs:"
            grep "Partition.*sample PKs" "$log_file" | head -5
        else
            echo -e "${YELLOW}⚠ No partition sample PK logs found${NC}"
        fi
        
        return 0
    else
        echo ""
        echo -e "${RED}✗ Migration failed in ${mode} mode${NC}"
        echo "Check log file: $log_file"
        return 1
    fi
}

# Function to truncate YugabyteDB table
truncate_table() {
    echo "Truncating YugabyteDB table..."
    docker exec -i yugabyte ysqlsh -h localhost -U yugabyte -d transaction_datastore <<EOF
TRUNCATE TABLE public.dda_pstd_fincl_txn_cnsmr_by_accntnbr;
\q
EOF
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Table truncated${NC}"
    else
        echo -e "${YELLOW}⚠ Failed to truncate table (may not exist yet)${NC}"
    fi
    echo ""
}

# Run tests
if [ "$TEST_MODE" == "COPY" ]; then
    truncate_table
    run_migration "COPY"
    TEST_RESULT=$?
elif [ "$TEST_MODE" == "INSERT" ]; then
    truncate_table
    run_migration "INSERT"
    TEST_RESULT=$?
elif [ "$TEST_MODE" == "BOTH" ]; then
    echo "Testing COPY mode first..."
    truncate_table
    run_migration "COPY"
    COPY_RESULT=$?
    
    echo ""
    echo "Waiting 5 seconds before testing INSERT mode..."
    sleep 5
    
    echo ""
    echo "Testing INSERT mode (should skip duplicates from COPY run)..."
    # Don't truncate - test duplicate handling
    run_migration "INSERT"
    INSERT_RESULT=$?
    
    if [ $COPY_RESULT -eq 0 ] && [ $INSERT_RESULT -eq 0 ]; then
        TEST_RESULT=0
        echo ""
        echo -e "${GREEN}✓ Both modes completed successfully${NC}"
    else
        TEST_RESULT=1
    fi
fi

echo ""
echo "=========================================="
if [ $TEST_RESULT -eq 0 ]; then
    echo -e "${GREEN}Test completed successfully${NC}"
else
    echo -e "${RED}Test failed${NC}"
fi
echo "=========================================="

exit $TEST_RESULT


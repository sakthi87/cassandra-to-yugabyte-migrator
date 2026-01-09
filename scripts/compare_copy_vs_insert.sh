#!/bin/bash

# Compare COPY vs INSERT mode performance
# Runs INSERT first, then COPY, captures metrics and compares

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo "=========================================="
echo "COPY vs INSERT Mode Performance Comparison"
echo "=========================================="
echo ""

# Check if Docker containers are running
echo "Checking Docker containers..."
if ! docker ps | grep -q yugabyte; then
    echo -e "${RED}ERROR: YugabyteDB container is not running${NC}"
    exit 1
fi
if ! docker ps | grep -q cassandra; then
    echo -e "${RED}ERROR: Cassandra container is not running${NC}"
    exit 1
fi
echo -e "${GREEN}✓${NC} Docker containers are running"
echo ""

# Check if JAR exists
JAR_FILE="target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar"
if [ ! -f "$JAR_FILE" ]; then
    echo -e "${YELLOW}Building JAR...${NC}"
    mvn package -DskipTests -q
fi
echo -e "${GREEN}✓${NC} JAR file ready"
echo ""

PROPERTIES_FILE="src/main/resources/migration.properties"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_DIR="performance_comparison_${TIMESTAMP}"
mkdir -p "$RESULTS_DIR"

# Function to extract metrics from log file
extract_metrics() {
    local log_file=$1
    local mode=$2
    
    # Extract timing information
    local start_time=$(grep -i "Starting execution\|Migration started\|Starting migration" "$log_file" | head -1 | awk '{print $1, $2}' || echo "")
    local end_time=$(grep -i "Migration completed\|completed successfully\|completed.*mode" "$log_file" | tail -1 | awk '{print $1, $2}' || echo "")
    
    # Extract row counts
    local rows_read=$(grep -i "rows.*read\|Rows Read:" "$log_file" | tail -1 | grep -oE "[0-9,]+" | head -1 | tr -d ',' || echo "0")
    local rows_written=$(grep -i "rows.*written\|Rows Written:" "$log_file" | tail -1 | grep -oE "[0-9,]+" | head -1 | tr -d ',' || echo "0")
    local rows_skipped=$(grep -i "rows.*skipped\|Rows Skipped:" "$log_file" | tail -1 | grep -oE "[0-9,]+" | head -1 | tr -d ',' || echo "0")
    
    # Extract partition information
    local partitions_completed=$(grep -i "Partition.*completed\|partitions.*completed" "$log_file" | wc -l | tr -d ' ' || echo "0")
    
    # Extract duplicate information (for INSERT mode)
    local duplicates_skipped="0"
    if [ "$mode" == "INSERT" ]; then
        duplicates_skipped=$(grep -i "duplicates.*skipped\|rows.*skipped.*duplicates" "$log_file" | tail -1 | grep -oE "[0-9,]+" | head -1 | tr -d ',' || echo "0")
    fi
    
    # Extract errors
    local errors=$(grep -i "ERROR\|Exception\|Failed" "$log_file" | grep -v "DEBUG" | wc -l | tr -d ' ' || echo "0")
    
    # Calculate duration (if timestamps are available)
    local duration=""
    if [ -n "$start_time" ] && [ -n "$end_time" ]; then
        # Try to calculate duration from timestamps (basic calculation)
        duration=$(grep -E "took.*seconds\|Duration:|Time taken:" "$log_file" | tail -1 | grep -oE "[0-9.]+" | head -1 || echo "")
    fi
    
    # Write metrics to file
    cat > "$RESULTS_DIR/${mode}_metrics.txt" <<EOF
Mode: $mode
Start Time: $start_time
End Time: $end_time
Duration: $duration
Rows Read: $rows_read
Rows Written: $rows_written
Rows Skipped: $rows_skipped
Duplicates Skipped: $duplicates_skipped
Partitions Completed: $partitions_completed
Errors: $errors
EOF
}

# Function to run migration and capture metrics
run_migration_with_metrics() {
    local mode=$1
    local log_file="$RESULTS_DIR/${mode}_migration_${TIMESTAMP}.log"
    
    echo "=========================================="
    echo -e "${CYAN}Running ${mode} Mode Migration${NC}"
    echo "=========================================="
    echo "Log file: $log_file"
    echo ""
    
    # Create temporary properties file
    local temp_props=$(mktemp)
    cp "$PROPERTIES_FILE" "$temp_props"
    
    # Update insertMode
    sed -i.bak "s/^yugabyte\.insertMode=.*/yugabyte.insertMode=${mode}/" "$temp_props"
    rm -f "${temp_props}.bak"
    
    echo "Configuration:"
    echo "  Mode: $mode"
    echo "  insertMode: $(grep "^yugabyte\.insertMode=" "$temp_props" | cut -d'=' -f2)"
    echo "  insertBatchSize: $(grep "^yugabyte\.insertBatchSize=" "$temp_props" | cut -d'=' -f2 || echo '1000')"
    echo ""
    
    # Record start time
    local start_epoch=$(date +%s)
    echo "Start time: $(date)"
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
    local end_epoch=$(date +%s)
    local duration_seconds=$((end_epoch - start_epoch))
    
    rm -f "$temp_props"
    
    echo ""
    echo "End time: $(date)"
    echo "Total duration: ${duration_seconds} seconds ($(echo "scale=2; $duration_seconds/60" | bc) minutes)"
    echo ""
    
    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}✓ ${mode} mode migration completed successfully${NC}"
        
        # Extract metrics
        extract_metrics "$log_file" "$mode"
        
        # Add duration to metrics file
        echo "Actual Duration (seconds): $duration_seconds" >> "$RESULTS_DIR/${mode}_metrics.txt"
        
        # Count rows from YugabyteDB
        echo "Counting rows in YugabyteDB..."
        local yugabyte_host=$(grep "^yugabyte.host=" "$PROPERTIES_FILE" | cut -d'=' -f2 | cut -d',' -f1)
        local yugabyte_port=$(grep "^yugabyte.port=" "$PROPERTIES_FILE" | cut -d'=' -f2)
        local yugabyte_db=$(grep "^yugabyte.database=" "$PROPERTIES_FILE" | cut -d'=' -f2)
        local yugabyte_user=$(grep "^yugabyte.username=" "$PROPERTIES_FILE" | cut -d'=' -f2)
        local yugabyte_pass=$(grep "^yugabyte.password=" "$PROPERTIES_FILE" | cut -d'=' -f2)
        local row_count=$(PGPASSWORD="$yugabyte_pass" psql -h "$yugabyte_host" -p "$yugabyte_port" -U "$yugabyte_user" -d "$yugabyte_db" -t -c "SELECT COUNT(*) FROM public.dda_pstd_fincl_txn_cnsmr_by_accntnbr;" 2>/dev/null | tr -d ' \n' || echo "0")
        echo "Rows in YugabyteDB: $row_count"
        echo "Rows in YugabyteDB: $row_count" >> "$RESULTS_DIR/${mode}_metrics.txt"
        
        return 0
    else
        echo -e "${RED}✗ ${mode} mode migration failed${NC}"
        return 1
    fi
}

# Function to truncate table
truncate_table() {
    echo "Truncating YugabyteDB table..."
    
    # Get connection details from properties
    local yugabyte_host=$(grep "^yugabyte.host=" "$PROPERTIES_FILE" | cut -d'=' -f2 | cut -d',' -f1)
    local yugabyte_port=$(grep "^yugabyte.port=" "$PROPERTIES_FILE" | cut -d'=' -f2)
    local yugabyte_db=$(grep "^yugabyte.database=" "$PROPERTIES_FILE" | cut -d'=' -f2)
    local yugabyte_user=$(grep "^yugabyte.username=" "$PROPERTIES_FILE" | cut -d'=' -f2)
    local yugabyte_pass=$(grep "^yugabyte.password=" "$PROPERTIES_FILE" | cut -d'=' -f2)
    
    # Use psql from host (works better than ysqlsh from inside container)
    local truncate_result=$(PGPASSWORD="$yugabyte_pass" psql -h "$yugabyte_host" -p "$yugabyte_port" -U "$yugabyte_user" -d "$yugabyte_db" -c "TRUNCATE TABLE public.dda_pstd_fincl_txn_cnsmr_by_accntnbr;" 2>&1)
    local exit_code=$?
    
    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}✓ Table truncated${NC}"
    else
        # Check if table doesn't exist
        if echo "$truncate_result" | grep -qi "does not exist\|relation.*does not exist"; then
            echo -e "${YELLOW}⚠ Table does not exist yet (will be created during migration)${NC}"
        else
            echo -e "${YELLOW}⚠ Truncate failed (may proceed anyway): $truncate_result${NC}"
        fi
    fi
    echo ""
}

# Function to compare metrics
compare_metrics() {
    echo ""
    echo "=========================================="
    echo -e "${CYAN}Performance Comparison${NC}"
    echo "=========================================="
    echo ""
    
    if [ ! -f "$RESULTS_DIR/INSERT_metrics.txt" ] || [ ! -f "$RESULTS_DIR/COPY_metrics.txt" ]; then
        echo -e "${RED}Error: Metrics files not found${NC}"
        return 1
    fi
    
    # Read metrics
    local insert_duration=$(grep "Actual Duration (seconds):" "$RESULTS_DIR/INSERT_metrics.txt" | cut -d':' -f2 | tr -d ' ')
    local copy_duration=$(grep "Actual Duration (seconds):" "$RESULTS_DIR/COPY_metrics.txt" | cut -d':' -f2 | tr -d ' ')
    
    local insert_rows=$(grep "^Rows Written:" "$RESULTS_DIR/INSERT_metrics.txt" | cut -d':' -f2 | tr -d ' ')
    local copy_rows=$(grep "^Rows Written:" "$RESULTS_DIR/COPY_metrics.txt" | cut -d':' -f2 | tr -d ' ')
    
    local insert_skipped=$(grep "^Duplicates Skipped:" "$RESULTS_DIR/INSERT_metrics.txt" | cut -d':' -f2 | tr -d ' ')
    
    # Calculate throughput (rows per second)
    local insert_throughput="0"
    local copy_throughput="0"
    if [ "$insert_duration" -gt 0 ] && [ "$insert_rows" -gt 0 ]; then
        insert_throughput=$(echo "scale=2; $insert_rows / $insert_duration" | bc)
    fi
    if [ "$copy_duration" -gt 0 ] && [ "$copy_rows" -gt 0 ]; then
        copy_throughput=$(echo "scale=2; $copy_rows / $copy_duration" | bc)
    fi
    
    # Calculate performance difference
    local duration_diff="0"
    local throughput_diff="0"
    local throughput_percent="0"
    if [ "$copy_throughput" != "0" ]; then
        duration_diff=$(echo "scale=2; $insert_duration - $copy_duration" | bc)
        throughput_diff=$(echo "scale=2; $insert_throughput - $copy_throughput" | bc)
        throughput_percent=$(echo "scale=2; ($insert_throughput / $copy_throughput) * 100" | bc)
    fi
    
    # Print comparison table
    printf "%-30s %15s %15s\n" "Metric" "INSERT Mode" "COPY Mode"
    echo "--------------------------------------------------------------"
    printf "%-30s %15s %15s\n" "Duration (seconds)" "$insert_duration" "$copy_duration"
    printf "%-30s %15s %15s\n" "Rows Written" "$insert_rows" "$copy_rows"
    printf "%-30s %15s %15s\n" "Throughput (rows/sec)" "$insert_throughput" "$copy_throughput"
    printf "%-30s %15s %15s\n" "Duplicates Skipped" "$insert_skipped" "N/A"
    echo ""
    
    echo "Comparison:"
    echo "  Duration difference: ${duration_diff} seconds ($(echo "scale=1; ($duration_diff / $copy_duration) * 100" | bc)% slower for INSERT)"
    echo "  Throughput difference: ${throughput_diff} rows/sec"
    echo "  INSERT throughput is ${throughput_percent}% of COPY throughput"
    echo ""
    
    # Write comparison to file
    cat > "$RESULTS_DIR/comparison.txt" <<EOF
Performance Comparison: INSERT vs COPY Mode
============================================

Metric                    INSERT Mode      COPY Mode
--------------------------------------------------------------
Duration (seconds)        $insert_duration        $copy_duration
Rows Written              $insert_rows        $copy_rows
Throughput (rows/sec)     $insert_throughput        $copy_throughput
Duplicates Skipped        $insert_skipped        N/A

Comparison:
- Duration difference: ${duration_diff} seconds ($(echo "scale=1; ($duration_diff / $copy_duration) * 100" | bc)% slower for INSERT)
- Throughput difference: ${throughput_diff} rows/sec
- INSERT throughput is ${throughput_percent}% of COPY throughput

Recommendation:
EOF
    
    if (( $(echo "$throughput_percent > 80" | bc -l) )); then
        echo "- INSERT mode performs well (${throughput_percent}% of COPY)" >> "$RESULTS_DIR/comparison.txt"
        echo "- Use INSERT mode for idempotent migrations (handles duplicates)" >> "$RESULTS_DIR/comparison.txt"
        echo -e "${GREEN}INSERT mode performs at ${throughput_percent}% of COPY mode${NC}"
    else
        echo "- INSERT mode is significantly slower (${throughput_percent}% of COPY)" >> "$RESULTS_DIR/comparison.txt"
        echo "- Consider using COPY mode for better performance" >> "$RESULTS_DIR/comparison.txt"
        echo -e "${YELLOW}INSERT mode is ${throughput_percent}% of COPY mode performance${NC}"
    fi
    
    echo ""
    echo "Full metrics saved to: $RESULTS_DIR/"
    echo "  - INSERT_metrics.txt"
    echo "  - COPY_metrics.txt"
    echo "  - comparison.txt"
}

# Main execution
echo "Step 1: Testing INSERT Mode"
echo "==========================="
truncate_table
run_migration_with_metrics "INSERT"
INSERT_RESULT=$?

if [ $INSERT_RESULT -ne 0 ]; then
    echo -e "${RED}INSERT mode test failed. Aborting.${NC}"
    exit 1
fi

echo ""
echo "Waiting 5 seconds before COPY mode test..."
sleep 5
echo ""

echo "Step 2: Testing COPY Mode"
echo "========================="
truncate_table
run_migration_with_metrics "COPY"
COPY_RESULT=$?

if [ $COPY_RESULT -ne 0 ]; then
    echo -e "${RED}COPY mode test failed.${NC}"
    exit 1
fi

# Compare metrics
compare_metrics

echo ""
echo "=========================================="
echo -e "${GREEN}Comparison Complete${NC}"
echo "=========================================="
echo "Results saved in: $RESULTS_DIR/"
echo ""

exit 0


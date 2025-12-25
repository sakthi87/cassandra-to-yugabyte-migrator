#!/bin/bash
# Wrapper script for analyze_migration_log.py
# Makes it easier to run the log analyzer

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT=$(dirname "$SCRIPT_DIR")

if [ $# -eq 0 ]; then
    echo "Usage: $0 <log_file_path> [output_file]"
    echo ""
    echo "Examples:"
    echo "  $0 migration_test_transaction_datastore.log"
    echo "  $0 /path/to/log.log reports/analysis.md"
    echo ""
    exit 1
fi

LOG_FILE="$1"
OUTPUT_FILE="$2"

# Check if log file exists
if [ ! -f "$LOG_FILE" ]; then
    echo "❌ Error: Log file not found: $LOG_FILE"
    exit 1
fi

# Run the analyzer
if [ -n "$OUTPUT_FILE" ]; then
    python3 "$SCRIPT_DIR/analyze_migration_log.py" "$LOG_FILE" --output "$OUTPUT_FILE"
else
    python3 "$SCRIPT_DIR/analyze_migration_log.py" "$LOG_FILE"
fi


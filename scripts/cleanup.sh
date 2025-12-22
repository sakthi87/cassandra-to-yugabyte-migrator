#!/bin/bash

# Script to cleanup migration artifacts
# Usage: ./scripts/cleanup.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=========================================="
echo "Migration Cleanup"
echo "=========================================="
echo "Project Directory: $PROJECT_DIR"
echo ""

# Cleanup options:
# 1. Remove checkpoint tables
# 2. Clean up temporary files
# 3. Reset migration state

read -p "Do you want to remove checkpoint tables? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Removing checkpoint tables..."
    # This would connect to YugabyteDB and drop checkpoint tables
    echo "Checkpoint table cleanup would be implemented here"
fi

read -p "Do you want to clean up temporary files? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Cleaning up temporary files..."
    find "$PROJECT_DIR" -type f -name "*.tmp" -delete
    find "$PROJECT_DIR" -type f -name "*.log" -delete
    echo "✅ Temporary files cleaned"
fi

echo ""
echo "✅ Cleanup completed"
echo ""


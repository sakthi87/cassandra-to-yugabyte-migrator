#!/bin/bash
echo "=== Finding Migration Log Files ==="
echo ""

echo "1. Current directory ($(pwd)):"
ls -lt *.log 2>/dev/null | head -5 || echo "  No .log files found"
echo ""

echo "2. Migration log files:"
ls -lt migration*.log 2>/dev/null | head -10 || echo "  No migration*.log files found"
echo ""

echo "3. Recent log files (last 24 hours, in current directory tree):"
find . -name "*.log" -type f -mtime -1 2>/dev/null | head -10 || echo "  No recent log files"
echo ""

echo "4. All .log files (current directory tree, max 20):"
find . -name "*.log" -type f 2>/dev/null | head -20 || echo "  No .log files found"
echo ""

echo "5. Check if migration is running:"
if pgrep -f "spark-submit.*MainApp" > /dev/null; then
  echo "  ✓ Migration process is running"
  echo "  PID: $(pgrep -f 'spark-submit.*MainApp')"
  echo "  Check terminal/output where it was started"
else
  echo "  No migration process found"
fi
echo ""

echo "=== To create log file for next run ==="
echo "Run: spark-submit ... > migration.log 2>&1"

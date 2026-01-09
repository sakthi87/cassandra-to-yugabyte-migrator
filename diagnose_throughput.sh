#!/bin/bash
LOG_FILE="${1:-migration.log}"

echo "=== Throughput Diagnostic Tool ==="
echo "Analyzing log file: $LOG_FILE"
echo ""

echo "1. ERROR COUNT:"
ERROR_COUNT=$(grep -ci "ERROR\|FAILED\|Exception" "$LOG_FILE" 2>/dev/null || echo "0")
echo "  Total errors: $ERROR_COUNT"
if [ "$ERROR_COUNT" -gt 0 ]; then
  echo "  Sample errors:"
  grep -i "ERROR\|FAILED\|Exception" "$LOG_FILE" | head -5 | sed 's/^/    /'
fi
echo ""

echo "2. PARTITION COMPLETIONS:"
INSERT_COMPLETIONS=$(grep -c "completed (INSERT mode)" "$LOG_FILE" 2>/dev/null || echo "0")
COPY_COMPLETIONS=$(grep -c "completed (COPY mode)" "$LOG_FILE" 2>/dev/null || echo "0")
echo "  INSERT mode completions: $INSERT_COMPLETIONS"
echo "  COPY mode completions: $COPY_COMPLETIONS"
echo ""

echo "3. RECENT COMPLETIONS (last 5):"
grep "completed.*mode" "$LOG_FILE" 2>/dev/null | tail -5 | sed 's/^/    /' || echo "  No completions found"
echo ""

echo "4. SPECIFIC ERRORS:"
echo "  Snapshot too old:"
grep -ci "snapshot too old" "$LOG_FILE" 2>/dev/null || echo "    0"
echo "  Serialization conflicts:"
grep -ci "serialization\|concurrent update" "$LOG_FILE" 2>/dev/null || echo "    0"
echo "  Connection errors:"
grep -ci "connection.*timeout\|connection.*refused" "$LOG_FILE" 2>/dev/null || echo "    0"
echo ""

echo "5. THROUGHPUT FROM LOGS:"
THROUGHPUT=$(grep -i "Throughput.*rows/sec" "$LOG_FILE" 2>/dev/null | tail -1)
if [ -n "$THROUGHPUT" ]; then
  echo "  $THROUGHPUT"
else
  echo "  Not found in logs yet (migration may still be running)"
fi
echo ""

echo "6. CONFIGURATION CHECK:"
if [ -f "migration.properties" ]; then
  echo "  Insert Mode: $(grep '^yugabyte.insertMode=' migration.properties 2>/dev/null | cut -d'=' -f2 || echo 'NOT SET')"
  echo "  Batch Size: $(grep '^yugabyte.insertBatchSize=' migration.properties 2>/dev/null | cut -d'=' -f2 || echo 'NOT SET')"
  echo "  Parallelism: $(grep '^spark.default.parallelism=' migration.properties 2>/dev/null | cut -d'=' -f2 || echo 'NOT SET')"
else
  echo "  migration.properties not found in current directory"
fi
echo ""

echo "7. DIAGNOSIS:"
if [ "$INSERT_COMPLETIONS" -eq 0 ] && [ "$COPY_COMPLETIONS" -eq 0 ]; then
  echo "  ⚠️  No partitions completed - check for errors or stuck partitions"
elif [ "$INSERT_COMPLETIONS" -lt 5 ]; then
  echo "  ⚠️  Very few partitions completed - performance issue"
elif [ "$ERROR_COUNT" -gt 10 ]; then
  echo "  ⚠️  High error count - check error messages above"
else
  echo "  ✅ Partitions completing, but throughput is low"
  echo "  → Check YBA UI for YSQL Ops/Sec (expected: 36-44 ops/sec)"
  echo "  → If YSQL Ops/Sec is 1-2 ops/sec: YugabyteDB bottleneck"
fi
echo ""

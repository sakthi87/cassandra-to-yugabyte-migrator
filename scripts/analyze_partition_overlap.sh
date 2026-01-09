#!/bin/bash

# Script to analyze partition overlap from migration logs
# Looks for duplicate primary keys across different partitions

LOG_FILE="${1:-migration_*.log}"

echo "=========================================="
echo "Partition Overlap Analysis"
echo "=========================================="
echo "Analyzing log file: $LOG_FILE"
echo ""

# Extract partition ID and sample PKs from logs
echo "Extracting partition information..."
grep "Partition.*sample PKs" "$LOG_FILE" | sed 's/.*Partition \([0-9]*\).*sample PKs.*: \(.*\)/Partition \1: \2/' > /tmp/partition_pks.txt

# Check for overlapping PKs across partitions
echo ""
echo "Checking for overlapping primary keys across partitions..."
echo ""

# Create a script to check for overlaps
python3 << 'PYTHON_SCRIPT'
import re
from collections import defaultdict

# Read partition data
partition_pks = defaultdict(set)
with open('/tmp/partition_pks.txt', 'r') as f:
    for line in f:
        match = re.match(r'Partition (\d+): (.*)', line.strip())
        if match:
            partition_id = match.group(1)
            pks_str = match.group(2)
            # Split PKs (comma-separated)
            pks = [pk.strip() for pk in pks_str.split(',') if pk.strip()]
            partition_pks[partition_id].update(pks)

# Find overlapping PKs
pk_to_partitions = defaultdict(list)
for partition_id, pks in partition_pks.items():
    for pk in pks:
        pk_to_partitions[pk].append(partition_id)

# Report overlaps
overlaps = {pk: partitions for pk, partitions in pk_to_partitions.items() if len(partitions) > 1}

if overlaps:
    print(f"⚠️  FOUND {len(overlaps)} OVERLAPPING PRIMARY KEYS:")
    print("=" * 60)
    for pk, partitions in list(overlaps.items())[:20]:  # Show first 20
        print(f"PK: {pk}")
        print(f"   Found in partitions: {', '.join(partitions)}")
        print()
    if len(overlaps) > 20:
        print(f"... and {len(overlaps) - 20} more overlaps")
    print("=" * 60)
    print("⚠️  This indicates multiple partitions are processing the same data!")
else:
    print("✅ No overlapping primary keys found in sampled data")
    print("   (Note: This only checks sampled PKs, not all data)")

# Summary
print("")
print("Summary:")
print(f"  Total partitions analyzed: {len(partition_pks)}")
print(f"  Total unique PKs sampled: {len(pk_to_partitions)}")
print(f"  Overlapping PKs: {len(overlaps)}")

PYTHON_SCRIPT

echo ""
echo "=========================================="
echo "Analysis Complete"
echo "=========================================="


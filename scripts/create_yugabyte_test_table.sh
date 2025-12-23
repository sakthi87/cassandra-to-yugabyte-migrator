#!/bin/bash
# Create corresponding table in YugabyteDB

set -e

echo "=========================================="
echo "Creating Test Table in YugabyteDB"
echo "=========================================="

# Create database (YugabyteDB doesn't support IF NOT EXISTS for CREATE DATABASE)
echo "Creating database..."
docker exec yugabyte bash -c 'cd /home/yugabyte/bin && ./ysqlsh -h $(hostname -i) -U yugabyte -d yugabyte -c "CREATE DATABASE test_migration;"' 2>&1 | grep -v "Warnings\|Note\|already exists" || true

# Create table in YugabyteDB (all fields NOT NULL to test the fix)
echo "Creating table in YugabyteDB..."
docker exec yugabyte bash -c 'cd /home/yugabyte/bin && ./ysqlsh -h $(hostname -i) -U yugabyte -d test_migration -c "
CREATE TABLE IF NOT EXISTS test_null_whitespace (
  id INTEGER PRIMARY KEY,
  normal_text TEXT NOT NULL,
  empty_string TEXT NOT NULL,
  whitespace_only TEXT NOT NULL,
  leading_trailing_spaces TEXT NOT NULL,
  non_ascii_text TEXT NOT NULL,
  null_value TEXT,
  mixed_content TEXT NOT NULL
);
"' 2>&1 | grep -v "Warnings\|Note:" || true

echo ""
echo "✅ YugabyteDB table created"
echo "Note: Most fields are NOT NULL to test the whitespace/non-ASCII fix"
echo "=========================================="


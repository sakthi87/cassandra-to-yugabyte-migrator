#!/bin/bash
# Create test table with various NULL, whitespace, and non-ASCII scenarios

set -e

echo "=========================================="
echo "Creating Test Table for NULL/Whitespace/Non-ASCII Testing"
echo "=========================================="

# Create keyspace if not exists
echo "Creating keyspace..."
docker exec cassandra cqlsh -e "
CREATE KEYSPACE IF NOT EXISTS test_migration
WITH REPLICATION = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};
" 2>&1 | grep -v "Warnings\|Note:" || true

# Create table in Cassandra
echo "Creating table in Cassandra..."
docker exec cassandra cqlsh -e "
USE test_migration;

CREATE TABLE IF NOT EXISTS test_null_whitespace (
  id INT PRIMARY KEY,
  normal_text TEXT,
  empty_string TEXT,
  whitespace_only TEXT,
  leading_trailing_spaces TEXT,
  non_ascii_text TEXT,
  null_value TEXT,
  mixed_content TEXT
);
" 2>&1 | grep -v "Warnings\|Note:" || true

# Insert 10 test records
echo "Inserting 10 test records..."
docker exec cassandra cqlsh -e "
USE test_migration;

-- Record 1: Normal values
INSERT INTO test_null_whitespace (id, normal_text, empty_string, whitespace_only, leading_trailing_spaces, non_ascii_text, null_value, mixed_content)
VALUES (1, 'normal_value', '', '   ', ' value_with_spaces ', 'café', null, 'mixed123');

-- Record 2: All whitespace variations
INSERT INTO test_null_whitespace (id, normal_text, empty_string, whitespace_only, leading_trailing_spaces, non_ascii_text, null_value, mixed_content)
VALUES (2, 'test', '', '     ', '   leading_and_trailing   ', '北京', null, 'test');

-- Record 3: Empty string in multiple fields
INSERT INTO test_null_whitespace (id, normal_text, empty_string, whitespace_only, leading_trailing_spaces, non_ascii_text, null_value, mixed_content)
VALUES (3, '', '', ' ', '', '東京', null, '');

-- Record 4: Non-ASCII in multiple fields
INSERT INTO test_null_whitespace (id, normal_text, empty_string, whitespace_only, leading_trailing_spaces, non_ascii_text, null_value, mixed_content)
VALUES (4, 'café', '', '   ', ' café ', 'München', null, 'résumé');

-- Record 5: Mixed whitespace and non-ASCII
INSERT INTO test_null_whitespace (id, normal_text, empty_string, whitespace_only, leading_trailing_spaces, non_ascii_text, null_value, mixed_content)
VALUES (5, 'normal', '', '   ', ' 北京 ', '上海', null, 'test 北京');

-- Record 6: Tab characters
INSERT INTO test_null_whitespace (id, normal_text, empty_string, whitespace_only, leading_trailing_spaces, non_ascii_text, null_value, mixed_content)
VALUES (6, 'tab\ttest', '', '	', '	tab_content	', 'São Paulo', null, 'tab\ttest');

-- Record 7: Special characters
INSERT INTO test_null_whitespace (id, normal_text, empty_string, whitespace_only, leading_trailing_spaces, non_ascii_text, null_value, mixed_content)
VALUES (7, 'special\"chars', '', '   ', ' \"quoted\" ', 'Zürich', null, 'test\"value');

-- Record 8: Only spaces in whitespace_only
INSERT INTO test_null_whitespace (id, normal_text, empty_string, whitespace_only, leading_trailing_spaces, non_ascii_text, null_value, mixed_content)
VALUES (8, 'test', '', '        ', '   ', 'Москва', null, 'test');

-- Record 9: Complex non-ASCII
INSERT INTO test_null_whitespace (id, normal_text, empty_string, whitespace_only, leading_trailing_spaces, non_ascii_text, null_value, mixed_content)
VALUES (9, 'complex', '', '   ', ' 复杂 ', 'العربية', null, 'test复杂');

-- Record 10: All edge cases
INSERT INTO test_null_whitespace (id, normal_text, empty_string, whitespace_only, leading_trailing_spaces, non_ascii_text, null_value, mixed_content)
VALUES (10, 'final', '', '     ', '  final test ', '日本語', null, 'final test');
" 2>&1 | grep -v "Warnings\|Note:" || true

echo ""
echo "Verifying records in Cassandra..."
docker exec cassandra cqlsh -e "
USE test_migration;
SELECT id, normal_text, empty_string, whitespace_only, leading_trailing_spaces, non_ascii_text, null_value, mixed_content FROM test_null_whitespace;
" 2>&1 | grep -v "Warnings\|Note:" | head -15

echo ""
echo "Record count:"
docker exec cassandra cqlsh -e "
USE test_migration;
SELECT COUNT(*) FROM test_null_whitespace;
" 2>&1 | grep -E "count|^[[:space:]]*[0-9]"

echo ""
echo "✅ Test table created with 10 records"
echo "=========================================="


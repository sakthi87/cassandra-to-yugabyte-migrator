#!/bin/bash

# Script to create test tables and insert sample data

set -e

KEYSPACE="test_keyspace"
TABLE="customer_transactions"

echo "=========================================="
echo "Creating Test Tables"
echo "=========================================="

# Create keyspace and table in Cassandra
echo ""
echo "Creating keyspace and table in Cassandra..."
docker exec -i cassandra cqlsh localhost 9042 <<EOF
CREATE KEYSPACE IF NOT EXISTS $KEYSPACE WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE $KEYSPACE;
DROP TABLE IF EXISTS $TABLE;
CREATE TABLE $TABLE (
    id UUID PRIMARY KEY,
    customer_id UUID,
    amount DECIMAL,
    txn_ts TIMESTAMP,
    status TEXT
);
INSERT INTO $TABLE (id, customer_id, amount, txn_ts, status) VALUES (uuid(), uuid(), 100.50, toTimestamp(now()), 'SUCCESS');
INSERT INTO $TABLE (id, customer_id, amount, txn_ts, status) VALUES (uuid(), uuid(), 250.75, toTimestamp(now()), 'SUCCESS');
INSERT INTO $TABLE (id, customer_id, amount, txn_ts, status) VALUES (uuid(), uuid(), 50.25, toTimestamp(now()), 'PENDING');
INSERT INTO $TABLE (id, customer_id, amount, txn_ts, status) VALUES (uuid(), uuid(), 500.00, toTimestamp(now()), 'SUCCESS');
INSERT INTO $TABLE (id, customer_id, amount, txn_ts, status) VALUES (uuid(), uuid(), 75.00, toTimestamp(now()), 'FAILED');
SELECT COUNT(*) FROM $TABLE;
EOF

echo ""
echo "Cassandra table created with sample data"

# Create database and table in YugabyteDB
echo ""
echo "Creating database and table in YugabyteDB..."
docker exec -i yugabyte bash -c '/home/yugabyte/bin/ysqlsh --host $(hostname) -U yugabyte -d yugabyte' <<EOF
-- Drop database if exists and recreate
DROP DATABASE IF EXISTS $KEYSPACE;
CREATE DATABASE $KEYSPACE;
\c $KEYSPACE
DROP TABLE IF EXISTS $TABLE CASCADE;
CREATE TABLE $TABLE (
    id UUID PRIMARY KEY,
    customer_id UUID,
    amount NUMERIC,
    txn_ts TIMESTAMPTZ,
    status TEXT
);
SELECT COUNT(*) FROM $TABLE;
EOF

echo ""
echo "YugabyteDB table created"
echo ""
echo "=========================================="
echo "✅ Test tables created successfully!"
echo "=========================================="
echo ""
echo "Cassandra: $KEYSPACE.$TABLE (5 records)"
echo "YugabyteDB: $KEYSPACE.$TABLE (0 records - ready for migration)"
echo ""


Great. Since the supervisor is healthy and tasks are ingesting, the next step is to verify that Druid has created segments and query the datasource.

Your datasource name from the supervisor is:

```text
yugabyte_logs
```

## 1. Check datasource exists

From the Druid UI:

**Query → Datasources**

You should see:

```text
yugabyte_logs
```

Check that:

* Segments > 0
* Size is increasing
* Latest segment interval is current

---

## 2. Run a basic SQL query

In Druid Console:

**Query → SQL**

Run:

```sql
SELECT
  COUNT(*) AS total_logs
FROM yugabyte_logs;
```

You should get a count greater than zero.

---

## 3. Check the latest ingested logs

Run:

```sql
SELECT
  __time,
  component,
  hostname,
  level,
  message
FROM yugabyte_logs
ORDER BY __time DESC
LIMIT 20;
```

Expected output should look like:

| __time         | component | hostname  | level | message               |
| -------------- | --------- | --------- | ----- | --------------------- |
| 2026-07-22T... | master    | yb-node-1 | I     | Started tablet server |
| 2026-07-22T... | tserver   | yb-node-1 | W     | RPC timeout           |

---

## 4. Check your metadata enrichment

Verify Fluent Bit fields:

```sql
SELECT
  component,
  environment,
  cloud,
  region,
  yugabyte_universe,
  owner,
  service,
  COUNT(*) AS logs
FROM yugabyte_logs
GROUP BY
  component,
  environment,
  cloud,
  region,
  yugabyte_universe,
  owner,
  service;
```

You should see values like:

```text
component             master
environment           development
cloud                 Azure
region                us-central
yugabyte_universe     yb-azure-dev
owner                 database-team
service               yugabyte
```

---

## 5. Check PostgreSQL audit logs

For your audit parser:

```sql
SELECT
  __time,
  username,
  database,
  client_host,
  client_port,
  errorcode,
  message
FROM yugabyte_logs
WHERE component = 'postgresql'
ORDER BY __time DESC
LIMIT 20;
```

---

## 6. Check if client_host transform worked

Run:

```sql
SELECT
  clienthostport,
  client_host,
  client_port
FROM yugabyte_logs
WHERE clienthostport IS NOT NULL
LIMIT 20;
```

Expected:

```
clienthostport          client_host       client_port
10.1.2.3:5433           10.1.2.3          5433
```

---

## 7. Check ingestion freshness

This is useful for monitoring:

```sql
SELECT
  MAX(__time) AS latest_log_time,
  CURRENT_TIMESTAMP AS current_time
FROM yugabyte_logs;
```

The two timestamps should be close.

---

## If you get no rows

Check:

1. Supervisor shows:

   ```
   Running Tasks: 2
   ```
2. MiddleManager/Indexer task logs show:

   ```
   Published segments
   ```
3. Query:

   ```sql
   SELECT * FROM sys.segments
   WHERE datasource = 'yugabyte_logs';
   ```

---

Given your earlier issue, the first query I would run is:

```sql
SELECT
  __time,
  event_timestamp,
  component,
  hostname,
  message
FROM yugabyte_logs
ORDER BY __time DESC
LIMIT 10;
```

That confirms the timestamp fix and ingestion path end-to-end.

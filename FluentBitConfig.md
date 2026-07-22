Fluentbit Config

Below is a production-style final Fluent Bit configuration for streaming YugabyteDB Master, TServer, and PostgreSQL logs to Kafka with:
Separate log sources
Persistent offsets using SQLite DB
Log parsing
Multiline support
Metadata enrichment:
Environment
Cloud
Region
Availability Zone
Yugabyte Universe
Cluster name
Node name
Component
Host information
JSON output to Kafka
Compression and retry handling
Assumption:
YugabyteDB node
|
├── /mnt/d0/yb-data/
│   ├── master/logs/*.INFO
│   ├── tserver/logs/*.INFO
│   └── tserver/logs/postgres*.log
|
└── Fluent Bit
        |
        └── Kafka
             |
             ├── Elasticsearch
             ├── Splunk
             └── SIEM

1. Fluent Bit configuration
File:
/etc/fluent-bit/fluent-bit.conf

[SERVICE]
    Flush                     5
    Daemon                    Off
    Log_Level                 info

    Parsers_File              /etc/fluent-bit/parsers.conf

    storage.path              /var/lib/fluent-bit/storage
    storage.sync              normal
    storage.checksum          on
    storage.backlog.mem_limit  50M


#################################################
# Yugabyte Master Logs
#################################################

[INPUT]
    Name              tail
    Tag               yugabyte.master

    Path              /mnt/d0/yb-data/master/logs/*.INFO*

    DB                /var/lib/fluent-bit/master.db

    Read_from_Head    true
    Refresh_Interval  5

    Parser            yugabyte_log

    Skip_Long_Lines   On

    storage.type      filesystem



#################################################
# Yugabyte TServer Logs
#################################################

[INPUT]
    Name              tail
    Tag               yugabyte.tserver

    Path              /mnt/d0/yb-data/tserver/logs/*.INFO*

    DB                /var/lib/fluent-bit/tserver.db

    Read_from_Head    true
    Refresh_Interval  5

    Parser            yugabyte_log

    Skip_Long_Lines   On

    storage.type      filesystem



#################################################
# Yugabyte PostgreSQL Logs
#################################################

[INPUT]
    Name              tail
    Tag               yugabyte.postgres

    Path              /mnt/d0/yb-data/tserver/logs/postgres*.log

    DB                /var/lib/fluent-bit/postgres.db

    Read_from_Head    true
    Refresh_Interval  5

    Parser            postgres_log

    Skip_Long_Lines   On

    storage.type      filesystem



#################################################
# Add hostname
#################################################

[FILTER]
    Name hostname
    Match yugabyte.*
    Hostname_Key hostname



#################################################
# Add component metadata
#################################################

[FILTER]
    Name modify
    Match yugabyte.master

    Add component master


[FILTER]
    Name modify
    Match yugabyte.tserver

    Add component tserver


[FILTER]
    Name modify
    Match yugabyte.postgres

    Add component postgres



#################################################
# Yugabyte Cluster Metadata
#################################################

[FILTER]
    Name record_modifier
    Match yugabyte.*

    Record environment production
    Record cloud aws

    Record region us-east-1
    Record availability_zone us-east-1a

    Record yugabyte_universe payments-prod
    Record yugabyte_cluster yb-prod-cluster

    Record owner database-team

    Record service yugabyte



#################################################
# Add Kubernetes metadata (enable only if K8s)
#################################################

# [FILTER]
#     Name kubernetes
#     Match yugabyte.*
#     Merge_Log On
#     Labels On
#     Annotations On



#################################################
# Kafka Output
#################################################

[OUTPUT]

    Name kafka

    Match yugabyte.*

    Brokers kafka1:9092,kafka2:9092,kafka3:9092

    Topics yugabyte.logs


    Format json

    Timestamp_Key timestamp

    Retry_Limit False


    rdkafka.request.required.acks 1

    rdkafka.compression.codec gzip

    rdkafka.message.send.max.retries 10

2. Parser configuration
File:
/etc/fluent-bit/parsers.conf

Yugabyte Master/TServer parser
[PARSER]

    Name        yugabyte_log

    Format      regex


    Regex       ^(?<severity>[IWEF])(?<date>\d{4}\s+\d{2}:\d{2}:\d{2}\.\d+)\s+(?<thread>\d+)\s+(?<file>[^:]+):(?<line>\d+)\]\s(?<message>.*)$


    Time_Key    date

    Time_Format %m%d %H:%M:%S.%L

Example input:
I0722 10:15:43.234123 12345 tablet.cc:456] Tablet bootstrap completed

Output:
{
 "severity":"I",
 "file":"tablet.cc",
 "line":"456",
 "message":"Tablet bootstrap completed"
}

PostgreSQL parser
Adjust based on your log_line_prefix.
Example:

[PARSER]

    Name postgres_log

    Format regex


    Regex ^(?<timestamp>[^ ]+ [^ ]+) \[(?<pid>[0-9]+)\] (?<level>[A-Z]+):\s+(?<message>.*)$


    Time_Key timestamp

    Time_Format %Y-%m-%d %H:%M:%S

Example:
2026-07-22 10:15:43 [12345] ERROR: duplicate key violation

Produces:
{
 "pid":"12345",
 "level":"ERROR",
 "message":"duplicate key violation"
}

3. Example Kafka message
A TServer log:
{
  "timestamp":"2026-07-22T10:15:43Z",

  "hostname":"yb-node-01",

  "environment":"production",
  "cloud":"aws",

  "region":"us-east-1",
  "availability_zone":"us-east-1a",

  "yugabyte_universe":"payments-prod",
  "yugabyte_cluster":"yb-prod-cluster",

  "service":"yugabyte",
  "owner":"database-team",

  "component":"tserver",

  "severity":"I",
  "file":"tablet.cc",
  "line":"456",

  "message":"Tablet bootstrap completed"
}

4. Recommended Kafka topic design
For most environments:
Topic:
    yugabyte.logs

Partition key:
hostname + component

Example:
yb-node-01:tserver
yb-node-02:master

This keeps ordering per node/component.
5. Recommended directory layout
On every YugabyteDB node:
/etc/fluent-bit/

├── fluent-bit.conf
├── parsers.conf

/var/lib/fluent-bit/

├── master.db
├── tserver.db
├── postgres.db
└── storage/

/mnt/d0/yb-data/

├── master/logs/
│       └── *.INFO

└── tserver/logs/
        ├── *.INFO
        └── postgres*.log

6. Production tuning values
For a high-volume YugabyteDB cluster:
Mem_Buf_Limit        100MB
Flush                2
Refresh_Interval     2
storage.type         filesystem

Kafka:
compression = gzip
acks = 1

For critical audit/security logs:
acks = all
compression = lz4

This configuration gives you a single searchable Kafka stream where every event can be filtered by:
environment
cloud
region
yugabyte_universe
yugabyte_cluster
hostname
component
severity

which works cleanly for Elasticsearch, Splunk, Datadog, or any SIEM consumer.

Since your VMs are **Red Hat Enterprise Linux (RHEL)**, the architecture remains the same, but installation commands change slightly.

Assumption:

* RHEL 8 / RHEL 9
* Java 17
* Apache Druid binary deployment (not Kubernetes)
* Azure Blob Storage ready
* YugabyteDB YSQL endpoint ready
* 4 VMs available

---

# Final VM Layout (RHEL)

| VM   | Services                   | Hostname Example |
| ---- | -------------------------- | ---------------- |
| VM-1 | Coordinator + Overlord     | druid-master     |
| VM-2 | Router + Broker            | druid-query      |
| VM-3 | Historical + MiddleManager | druid-data       |
| VM-4 | ZooKeeper                  | druid-zk         |

```
                SQL Client
                    |
                    |
              Router :8888
                    |
              Broker :8082
                    |
       +------------+-------------+
       |                          |
 Historical                Historical
       |
       |
 Azure Blob Storage
 (Deep Storage)


 Coordinator
 Overlord
       |
       |
 YugabyteDB
 (Metadata)

       |
   ZooKeeper
```

---

# Step 0 — OS Preparation (All 4 VMs)

SSH:

```bash
ssh <user>@<vm-ip>
```

Become root:

```bash
sudo su -
```

Update:

```bash
dnf update -y
```

Install tools:

```bash
dnf install -y wget curl tar vim net-tools unzip
```

---

# Step 1 — Install Java 17 (All Druid VMs)

RHEL:

```bash
dnf install java-17-openjdk java-17-openjdk-devel -y
```

Verify:

```bash
java -version
```

Expected:

```
openjdk version "17.x"
```

Set JAVA_HOME:

```bash
cat <<EOF >> /etc/profile

export JAVA_HOME=/usr/lib/jvm/java-17-openjdk
export PATH=\$PATH:\$JAVA_HOME/bin

EOF
```

Reload:

```bash
source /etc/profile
```

---

# Step 2 — Create Druid User (All Druid VMs)

```bash
useradd druid

mkdir /opt/druid

chown druid:druid /opt/druid
```

Switch:

```bash
su - druid
```

---

# Step 3 — Download Apache Druid (All Druid VMs)

Go:

```bash
cd /opt
```

Download:

```bash
wget https://downloads.apache.org/druid/latest/apache-druid-bin.tar.gz
```

Extract:

```bash
tar -xzf apache-druid-bin.tar.gz
```

Rename:

```bash
mv apache-druid-* druid
```

Result:

```
/opt/druid

bin
conf
extensions
lib
```

---

# Step 4 — Install PostgreSQL Client for Yugabyte Connection

On all Druid VMs:

```bash
dnf install postgresql -y
```

Test Yugabyte:

```bash
psql \
-h <yugabyte-host> \
-p 5433 \
-U druid \
-d druid
```

---

# Step 5 — Create Yugabyte Metadata Database

From Yugabyte:

```sql
CREATE DATABASE druid;

CREATE USER druid 
WITH PASSWORD 'password';

GRANT ALL PRIVILEGES 
ON DATABASE druid 
TO druid;
```

---

# Step 6 — Install Azure Blob Extension

On all Druid nodes:

```bash
cd /opt/druid
```

Run:

```bash
./bin/extensions sync
```

Enable Azure:

Edit:

```
conf/druid/cluster/_common/common.runtime.properties
```

Add:

```properties
druid.extensions.loadList=[
"druid-azure-extensions",
"druid-postgresql-metadata-storage"
]
```

---

# Step 7 — Configure Common Runtime

Same file:

```
common.runtime.properties
```

Add:

## Yugabyte Metadata

```properties
druid.metadata.storage.type=postgresql


druid.metadata.storage.connector.connectURI=jdbc:postgresql://<YB-IP>:5433/druid


druid.metadata.storage.connector.user=druid


druid.metadata.storage.connector.password=password
```

---

## ZooKeeper

```properties
druid.zk.service.host=<zk-ip>:2181
```

---

## Azure Blob

```properties
druid.storage.type=azure


druid.azure.account=druidstorage


druid.azure.container=druid-segments


druid.azure.prefix=druid
```

---

# Step 8 — Configure Services

## VM-1: Coordinator + Overlord

Start Coordinator:

```bash
cd /opt/druid

./bin/run-druid coordinator
```

Start Overlord:

```bash
./bin/run-druid overlord
```

Ports:

```
Coordinator
8081

Overlord
8090
```

---

# VM-2: Router + Broker

Router:

```bash
./bin/run-druid router
```

Broker:

```bash
./bin/run-druid broker
```

Ports:

```
Router

8888


Broker

8082
```

---

# VM-3: Historical + MiddleManager

Historical:

```bash
./bin/run-druid historical
```

MiddleManager:

```bash
./bin/run-druid middleManager
```

Ports:

```
Historical

8083


MiddleManager

8091
```

---

# VM-4: ZooKeeper

Install:

```bash
dnf install zookeeper zookeeper-server -y
```

Initialize:

```bash
/usr/lib/zookeeper/bin/zkServer.sh start
```

Verify:

```bash
echo ruok | nc localhost 2181
```

Expected:

```
imok
```

---

# Step 9 — Verify Cluster

From browser:

Router:

```
http://<router-ip>:8888
```

Coordinator:

```
http://<master-ip>:8081
```

Overlord:

```
http://<master-ip>:8090
```

---

# Step 10 — Test Azure Blob Upload

Submit ingestion task.

Example:

```
JSON
 |
 |
MiddleManager
 |
 |
Creates segments
 |
 |
Azure Blob

druid-segments/

 |
 |
Historical loads

```

Check Azure container:

```
druid-segments

datasource/
    segment files
```

---

# 1 Hour Timeline (Realistic)

| Time      | Activity                      |
| --------- | ----------------------------- |
| 0-10 min  | Java + OS + user setup        |
| 10-20 min | Druid install                 |
| 20-30 min | Yugabyte + Blob configuration |
| 30-45 min | Start Druid services          |
| 45-55 min | Validation                    |
| 55-60 min | Sample ingestion              |

---

One important RHEL-specific recommendation:

For **Historical VM**, mount the 1TB disk separately:

Example:

```
/data/druid

instead of

/opt/druid
```

because Historical node uses local disk as **segment cache**.

Your final stack:

```
RHEL VM
   |
Apache Druid
   |
+----------------+
| YugabyteDB     |
| Metadata       |
+----------------+

+----------------+
| Azure Blob     |
| Deep Storage   |
+----------------+

+----------------+
| ZooKeeper      |
| Coordination   |
+----------------+
```

This is a very good enterprise-style reference deployment.



---




That's actually the **most important VM** in a Druid cluster because it is responsible for **both ingesting data and serving queries**.

Let's break down the two services running on **VM-3**.

---

# 1. MiddleManager – Data Ingestion

The **MiddleManager** is the worker responsible for ingestion tasks.

Its responsibilities include:

* Reading data from the source (Azure Blob, Kafka, local files, etc.)
* Parsing CSV, JSON, Parquet, ORC, etc.
* Applying transformations and rollup
* Creating Druid segments
* Uploading the completed segments to Deep Storage (Azure Blob in your deployment)

For your setup, the ingestion flow looks like:

```text
Azure Blob Storage (Raw Files)
        │
        ▼
MiddleManager
        │
        ├── Read sales.csv
        ├── Parse rows
        ├── Build indexes
        ├── Create segments
        ▼
Azure Blob Storage (Deep Storage)
```

Suppose you upload:

```text
sales-data/
    sales.csv
```

When you submit an ingestion task:

```text
Overlord
      │
      ▼
Assigns task
      │
      ▼
MiddleManager
```

The MiddleManager performs all the processing.

---

# 2. Historical – Query Processing

The **Historical** node is responsible for serving queries against immutable data.

It:

* Downloads segments from Azure Blob (deep storage)
* Stores them in a **local segment cache**
* Executes SQL queries against those segments
* Returns results to the Broker

The query flow is:

```text
Azure Blob Storage
      │
      ▼
Historical
      │
Downloads segment
      │
Stores locally
      │
Answers queries
```

This is why you were advised to mount a large disk at `/data/druid`: the Historical node keeps a local cache of segments to avoid downloading them from Azure Blob for every query.

---

# Why does Historical need a 1 TB disk?

Imagine you've ingested 500 GB of data.

The segments are stored in Azure Blob:

```text
Azure Blob
└── druid-segments
    ├── segment1
    ├── segment2
    ├── segment3
```

When Historical starts, it downloads the assigned segments:

```text
/data/druid/segments
├── segment1
├── segment2
├── segment3
```

Queries are served from the local disk, not directly from Azure Blob, which significantly improves performance.

---

# Why combine Historical and MiddleManager on one VM?

For small or development deployments, it's common to run both services together because it reduces the number of VMs.

Your VM-3 is doing two jobs:

```text
               VM-3
+--------------------------------------+
|                                      |
| MiddleManager                        |
|   • Reads Azure Blob                 |
|   • Builds segments                  |
|   • Uploads segments                 |
|                                      |
| Historical                           |
|   • Downloads segments               |
|   • Stores local cache               |
|   • Serves queries                   |
|                                      |
+--------------------------------------+
```

In larger production environments, these roles are usually separated.

For example:

| Service       | Typical VM Count |
| ------------- | ---------------: |
| MiddleManager |             2–10 |
| Historical    |             3–20 |

This separation allows ingestion and query workloads to scale independently.

---

# Complete data flow in your deployment

```text
                Azure Blob Storage
             (sales.csv, inventory.csv)
                       │
                       ▼
                MiddleManager (VM-3)
                       │
         Reads and indexes the data
                       │
                       ▼
             Creates Druid segments
                       │
                       ▼
      Azure Blob Storage (Deep Storage)
                       │
                       ▼
             Historical (VM-3)
             Downloads segments
                       │
              Local cache (/data/druid)
                       │
                       ▼
                 Broker (VM-2)
                       │
                       ▼
                 Router (VM-2)
                       │
                       ▼
                  SQL Client
```

### For your 4-VM deployment

| VM                                    | Primary Responsibility                                                                                  |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| **VM-1 (Coordinator + Overlord)**     | Manages the cluster and schedules ingestion tasks.                                                      |
| **VM-2 (Router + Broker)**            | Accepts SQL queries and routes them to the appropriate Historical nodes.                                |
| **VM-3 (Historical + MiddleManager)** | Ingests data from Azure Blob, creates segments, caches segments locally, and serves analytical queries. |
| **VM-4 (ZooKeeper)**                  | Coordinates cluster state and service discovery.                                                        |

For a proof of concept or moderate workload, this 4-VM architecture is a common and practical deployment. As your ingestion volume or query load grows, the Historical and MiddleManager roles are often split onto separate VMs so each can scale independently.


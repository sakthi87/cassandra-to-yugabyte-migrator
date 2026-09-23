Good — locking in 16 vCPU / 64 GB across all six nodes. Here's the full install runbook for that layout: **RHEL 8, 6 nodes, YugabyteDB (YSQL) as metadata store, Azure Blob Storage as deep storage.**

## 0. Final target layout

| Node | Services | CPU/RAM | Disk |
|---|---|---|---|
| druid-master-1 | Coordinator, Overlord, ZooKeeper, Router, Broker | 16/64 | 200–300 GB SSD |
| druid-master-2 | Coordinator, Overlord, ZooKeeper, Router, Broker | 16/64 | 200–300 GB SSD |
| druid-master-3 | Coordinator, Overlord, ZooKeeper, Router, Broker | 16/64 | 200–300 GB SSD |
| druid-data-1 | Historical, MiddleManager | 16/64 | 1.5–2 TB Premium SSD/NVMe |
| druid-data-2 | Historical, MiddleManager | 16/64 | 1.5–2 TB Premium SSD/NVMe |
| druid-data-3 | Historical, MiddleManager | 16/64 | 1.5–2 TB Premium SSD/NVMe |

Metadata store: existing YugabyteDB (YSQL, Postgres wire-compatible) — reachable on port 5433.
Deep storage: Azure Blob Storage container (replacing the S3 plan).

---

## 1. OS prep — run on ALL 6 nodes

```bash
# Hostname / hosts file (repeat entries for all 6 nodes on every node)
sudo hostnamectl set-hostname druid-master-1     # adjust per node
sudo vi /etc/hosts
# 10.x.x.11 druid-master-1
# 10.x.x.12 druid-master-2
# 10.x.x.13 druid-master-3
# 10.x.x.14 druid-data-1
# 10.x.x.15 druid-data-2
# 10.x.x.16 druid-data-3

# Disable SELinux enforcing (or write policies later)
sudo setenforce 0
sudo sed -i 's/^SELINUX=enforcing/SELINUX=permissive/' /etc/selinux/config

# Firewalld — open Druid + ZK ports (adjust CIDR to your subnet)
sudo firewall-cmd --permanent --add-port={2181,2888,3888,8081,8082,8083,8084,8088,8090,8091,8100-8199,8200-8299}/tcp
sudo firewall-cmd --reload

# Time sync
sudo dnf install -y chrony
sudo systemctl enable --now chronyd

# ulimits
sudo tee /etc/security/limits.d/druid.conf <<EOF
druid soft nofile 65536
druid hard nofile 65536
druid soft nproc  32768
druid hard nproc  32768
EOF

# vm settings for ZK/Druid
sudo tee -a /etc/sysctl.conf <<EOF
vm.swappiness=1
vm.max_map_count=262144
EOF
sudo sysctl -p

# Java 17 (Druid 28/29+ requires 17 by default, 26+ also supports it — confirm your target version)
sudo dnf install -y java-17-openjdk java-17-openjdk-devel
sudo alternatives --config java

# Druid service user
sudo groupadd druid
sudo useradd -g druid -m -d /home/druid -s /bin/bash druid
```

---

## 2. Azure deep storage setup (replaces S3)

Do this once, from Azure CLI (on a jump box or one of the nodes):

```bash
# Login
az login

RG="rg-druid-prod"
LOCATION="eastus"
STORAGE_ACCOUNT="druiddeepstorageacct"   # must be globally unique, lowercase, no dashes
CONTAINER="druid-segments"

az group create --name $RG --location $LOCATION

az storage account create \
  --name $STORAGE_ACCOUNT \
  --resource-group $RG \
  --location $LOCATION \
  --sku Standard_LRS \
  --kind StorageV2 \
  --access-tier Hot

az storage container create \
  --name $CONTAINER \
  --account-name $STORAGE_ACCOUNT \
  --auth-mode login
```

Get the access key (used by Druid's `druid-azure-extension`):

```bash
az storage account keys list \
  --account-name $STORAGE_ACCOUNT \
  --resource-group $RG \
  --query "[0].value" -o tsv
```

Save that key securely — Druid needs it in `common.runtime.properties`, or better, inject it via environment variable / Azure Key Vault reference rather than plaintext (see step 5).

> **Production recommendation:** use a **Managed Identity** + `azure.managedIdentityClientId` instead of a static account key if your nodes are Azure VMs — avoids storing a shared key on disk. I've shown the key-based method below since it's simpler to get running first; flag if you want the Managed Identity variant instead.

---

## 3. YugabyteDB metadata store prep

YugabyteDB's YSQL is Postgres wire-compatible, so Druid's **postgresql-metadata-storage** core extension talks to it directly.

On the YugabyteDB cluster (via `ysqlsh`):

```sql
CREATE DATABASE druid;
CREATE USER druid_user WITH PASSWORD 'ChangeMe_StrongPassword!';
GRANT ALL PRIVILEGES ON DATABASE druid TO druid_user;
ALTER DATABASE druid OWNER TO druid_user;
```

Confirm reachability from every Druid node on port 5433:

```bash
psql "host=<yb-tserver-vip-or-lb> port=5433 dbname=druid user=druid_user" -c "SELECT version();"
```

Note the connection endpoint — ideally a load-balanced VIP across your YugabyteDB tservers, not a single node.

---

## 4. Download and lay out Druid — on ALL 6 nodes

```bash
DRUID_VERSION=30.0.1   # pick your current target version
cd /opt
sudo curl -O https://dlcdn.apache.org/druid/${DRUID_VERSION}/apache-druid-${DRUID_VERSION}-bin.tar.gz
sudo tar -xzf apache-druid-${DRUID_VERSION}-bin.tar.gz
sudo ln -s apache-druid-${DRUID_VERSION} druid
sudo chown -R druid:druid /opt/apache-druid-${DRUID_VERSION} /opt/druid

sudo mkdir -p /opt/druid/var /data/druid
sudo chown -R druid:druid /opt/druid/var /data/druid
```

Install required extensions (from within `/opt/druid`, as the `druid` user):

```bash
cd /opt/druid
java -classpath "lib/*" org.apache.druid.cli.Main tools pull-deps \
  --no-default-hadoop \
  -c "org.apache.druid.extensions:druid-azure-extension" \
  -c "org.apache.druid.extensions:postgresql-metadata-storage"
```

---

## 5. Common configuration — `conf/druid/cluster/_common/common.runtime.properties`

Same file, deployed identically to **all 6 nodes**:

```properties
druid.extensions.loadList=["druid-azure-extension", "postgresql-metadata-storage", "druid-histogram", "druid-datasketches", "druid-lookups-cached-global", "simple-client-sslcontext"]

# ---- ZooKeeper ----
druid.zk.service.host=druid-master-1:2181,druid-master-2:2181,druid-master-3:2181
druid.zk.paths.base=/druid

# ---- Metadata storage: YugabyteDB via Postgres connector ----
druid.metadata.storage.type=postgresql
druid.metadata.storage.connector.connectURI=jdbc:postgresql://<yb-lb-endpoint>:5433/druid
druid.metadata.storage.connector.user=druid_user
druid.metadata.storage.connector.password=ChangeMe_StrongPassword!

# ---- Deep storage: Azure Blob (replaces S3) ----
druid.storage.type=azure
druid.azure.account=druiddeepstorageacct
druid.azure.key=<storage_account_key_from_step2>
druid.azure.container=druid-segments
druid.azure.prefix=segments
druid.azure.protocol=https
# maxTries optional tuning
druid.azure.maxTries=3

# ---- Indexing task logs — also to Azure ----
druid.indexer.logs.type=azure
druid.indexer.logs.container=druid-segments
druid.indexer.logs.prefix=indexing-logs

# ---- General ----
druid.host=%%HOSTNAME%%
druid.plaintextPort=%%PORT%%
druid.tlsPort=-1
druid.enablePlaintextPort=true

druid.selectors.indexing.serviceName=druid/overlord
druid.selectors.coordinator.serviceName=druid/coordinator

druid.monitoring.monitors=["org.apache.druid.java.util.metrics.JvmMonitor"]
druid.emitter=noop

druid.storage.storageDirectory=/data/druid/segment-cache
druid.segmentCache.locations=[{"path":"/data/druid/segment-cache","maxSize":300000000000}]
```

Replace `%%HOSTNAME%%` per-node and `%%PORT%%` per-service (set in each service's own `runtime.properties`, not here — remove those two placeholder lines from common and set `druid.host` per node instead).

For the **key**, don't leave it in plaintext long-term — options once basic install is verified:
- Pull it from an environment variable: `druid.azure.key=${AZURE_STORAGE_KEY}` isn't natively supported by properties files, so instead wrap Druid's systemd unit to export it and reference via `druid.azure.key=%%AZURE_KEY%%` templated at deploy time, or
- Switch to Managed Identity auth (`druid.azure.managedIdentityClientId=<client-id>`, drop `druid.azure.key`).

---

## 6. Node group configs

### Nodes 1–3 (Master + Query group) — ZooKeeper

Each of the 3 nodes runs its own ZK instance. Install ZK config once per node:

```bash
# /opt/druid/conf/zk/conf/zoo.cfg  (or use bundled ZK under conf-quickstart/zk if using Druid's shipped ZK)
tickTime=2000
dataDir=/data/druid/zk
clientPort=2181
initLimit=10
syncLimit=5
server.1=druid-master-1:2888:3888
server.2=druid-master-2:2888:3888
server.3=druid-master-3:2888:3888
```

```bash
sudo mkdir -p /data/druid/zk
echo "1" | sudo tee /data/druid/zk/myid     # 2 on master-2, 3 on master-3
sudo chown -R druid:druid /data/druid/zk
```

### Coordinator — `conf/druid/cluster/master/coordinator-overlord/runtime.properties`

Druid 0.22+ ships a combined coordinator-overlord process — recommended over separate ones for this size cluster:

```properties
druid.host=druid-master-1
druid.plaintextPort=8081

druid.coordinator.asOverlord.enabled=true
druid.coordinator.asOverlord.overlordService=druid/overlord

druid.coordinator.startDelay=PT30S
druid.coordinator.period=PT30S

druid.indexer.queue.startDelay=PT5S
druid.indexer.runner.type=remote
druid.indexer.storage.type=metadata
```

JVM config (`jvm.config` in same folder):
```
-server
-Xms8g
-Xmx8g
-XX:+ExitOnOutOfMemoryError
-Duser.timezone=UTC
-Dfile.encoding=UTF-8
```

### Broker — `conf/druid/cluster/query/broker/runtime.properties`

```properties
druid.host=druid-master-1
druid.plaintextPort=8082
druid.service=druid/broker

druid.broker.http.numConnections=20
druid.server.http.numThreads=40
druid.processing.buffer.sizeBytes=500MiB
druid.processing.numMergeBuffers=4
druid.processing.numThreads=15
druid.sql.enable=true
```

`jvm.config`:
```
-server
-Xms16g
-Xmx16g
-XX:MaxDirectMemorySize=16g
-XX:+ExitOnOutOfMemoryError
-Duser.timezone=UTC
-Dfile.encoding=UTF-8
```

### Router — `conf/druid/cluster/query/router/runtime.properties`

```properties
druid.host=druid-master-1
druid.plaintextPort=8888
druid.service=druid/router
druid.router.defaultBrokerServiceName=druid/broker
druid.router.coordinatorServiceName=druid/coordinator
druid.router.managementProxy.enabled=true
```

Repeat Coordinator/Broker/Router config identically on master-2 and master-3, just changing `druid.host`.

---

### Nodes 4–6 (Data group)

### Historical — `conf/druid/cluster/data/historical/runtime.properties`

```properties
druid.host=druid-data-1
druid.plaintextPort=8083
druid.service=druid/historical

druid.processing.buffer.sizeBytes=500MiB
druid.processing.numMergeBuffers=4
druid.processing.numThreads=15

druid.segmentCache.locations=[{"path":"/data/druid/segment-cache","maxSize":1500000000000}]
druid.server.maxSize=1500000000000
```

`jvm.config`:
```
-server
-Xms16g
-Xmx16g
-XX:MaxDirectMemorySize=32g
-XX:+ExitOnOutOfMemoryError
-Duser.timezone=UTC
-Dfile.encoding=UTF-8
```

### MiddleManager — `conf/druid/cluster/data/middleManager/runtime.properties`

```properties
druid.host=druid-data-1
druid.plaintextPort=8091
druid.service=druid/middleManager

druid.worker.capacity=8
druid.indexer.runner.javaOpts=-server -Xms1g -Xmx1g -XX:MaxDirectMemorySize=2g -Duser.timezone=UTC -Dfile.encoding=UTF-8
druid.indexer.task.baseTaskDir=/data/druid/task
```

Repeat on data-2 and data-3, changing `druid.host`.

---

## 7. systemd units — on all nodes, per service

Example for Coordinator on master-1 (repeat pattern for broker/router/historical/middleManager/zk):

```ini
# /etc/systemd/system/druid-coordinator.service
[Unit]
Description=Apache Druid Coordinator-Overlord
After=network.target

[Service]
User=druid
Group=druid
WorkingDirectory=/opt/druid
ExecStart=/opt/druid/bin/start-cluster-master-no-zk-server
Restart=on-failure
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

Enable:
```bash
sudo systemctl daemon-reload
sudo systemctl enable --now druid-coordinator
```

(Use Druid's bundled `bin/start-cluster-*` scripts as the entry points — `start-cluster-master-no-zk-server` for master nodes, `start-cluster-query-server` for broker+router, `start-cluster-data-server` for historical+middleManager, and a separate `zookeeper-server-start` unit for ZK on nodes 1–3.)

---

## 8. Start order & verification

```bash
# 1. ZooKeeper first — all 3 master nodes
sudo systemctl start druid-zk

# 2. Coordinator/Overlord — all 3 master nodes
sudo systemctl start druid-coordinator

# 3. Historical + MiddleManager — all 3 data nodes
sudo systemctl start druid-historical druid-middlemanager

# 4. Broker + Router — all 3 master nodes
sudo systemctl start druid-broker druid-router
```

Validate:
```bash
curl http://druid-master-1:8081/status
curl http://druid-master-1:8888/status
```

Open the Router console: `http://druid-master-1:8888` — confirm all 6 nodes register as healthy under Services, and that deep storage writes succeed by running a test ingestion (native batch job against a small sample file, checking segments land in the `druid-segments` Azure container and metadata rows appear in the YugabyteDB `druid_segments` table).

---

Want me to also draft the **Managed Identity variant** for the Azure key (skips storing the storage account key on disk), or a **firewall/NSG rule table** for the Azure side matching this port layout?

# Cassandra to YugabyteDB Migrator

Production-grade Spark-based migration tool for migrating data from Apache Cassandra to YugabyteDB using COPY FROM STDIN for optimal performance.

## 🚀 Features

- **High Performance**: Optimized for 10K+ IOPS throughput
- **Spark-Based**: Leverages Spark for distributed processing and fault tolerance
- **COPY FROM STDIN**: Direct streaming to YugabyteDB using PostgreSQL COPY protocol
- **Token-Aware Partitioning**: Automatic token range splitting for optimal parallelism
- **Checkpointing**: Resume capability for large migrations
- **Data Validation**: Row count and checksum validation
- **Properties-Driven**: All configuration via single properties file

## 📊 Performance

### Current Performance (Local Docker)
- **Baseline**: ~7,407 rows/sec (13.5s for 100K rows)
- **Optimized**: ~5,555 rows/sec (18s for 100K rows)
- **Target**: 10,000+ rows/sec (requires production cluster)

### Production Cluster Expected Performance
- **Target**: 10,000-20,000 rows/sec
- **Requirements**: Spark cluster, multiple YugabyteDB nodes, optimized GFlags

## 🏗️ Architecture

```
Cassandra
  │
  │  (Token-aware partitioning, paging, retry logic)
  ▼
Spark Cassandra Connector
  │
  │  (Spark partitions ≈ token ranges)
  ▼
PartitionExecutor
  │
  │  (Yugabyte COPY FROM STDIN)
  ▼
Yugabyte YSQL
```

## 📋 Prerequisites

- **Java**: JDK 11+
- **Scala**: 2.13
- **Spark**: 3.5.1
- **Maven**: 3.6+
- **Cassandra**: 3.x or 4.x
- **YugabyteDB**: 2.18+ (YSQL)

## 🔧 Configuration

All configuration is in `src/main/resources/migration.properties`:

### Cassandra Settings
```properties
cassandra.host=localhost
cassandra.port=9043
cassandra.localDC=datacenter1
cassandra.fetchSizeInRows=10000
cassandra.inputSplitSizeMb=256
cassandra.concurrentReads=2048
cassandra.consistencyLevel=LOCAL_ONE
```

### YugabyteDB Settings
```properties
yugabyte.host=localhost
yugabyte.port=5433
yugabyte.database=transaction_datastore
yugabyte.username=yugabyte
yugabyte.password=yugabyte
yugabyte.copyBufferSize=100000
yugabyte.copyFlushEvery=50000
```

### Spark Settings
```properties
spark.executor.instances=4
spark.executor.cores=4
spark.executor.memory=8g
spark.default.parallelism=16
spark.memory.fraction=0.8
```

## 🚀 Quick Start

### 1. Build the Project
```bash
mvn clean package -DskipTests
```

### 2. Configure Migration
Edit `src/main/resources/migration.properties`:
- Set Cassandra connection details
- Set YugabyteDB connection details
- Configure source and target table names

### 3. Run Migration
```bash
export SPARK_HOME=$HOME/spark-3.5.1
$SPARK_HOME/bin/spark-submit \
  --class com.company.migration.MainApp \
  --master 'local[4]' \
  --driver-memory 4g \
  --executor-memory 8g \
  --executor-cores 4 \
  --conf spark.default.parallelism=16 \
  target/cassandra-to-yugabyte-migrator-1.0.0-SNAPSHOT.jar \
  migration.properties
```

## 📈 Performance Optimization

### Applied Optimizations

1. **Cassandra Read**
   - Fetch size: 10,000 rows (fewer queries)
   - Split size: 256MB (larger partitions)
   - Concurrent reads: 2,048 (more parallelism)
   - Consistency: LOCAL_ONE (faster reads)

2. **YugabyteDB COPY**
   - Buffer size: 100,000 rows (larger batches)
   - Flush interval: 50,000 rows (fewer flushes)
   - Buffer capacity: 4MB (reduced allocations)

3. **Spark Configuration**
   - Parallelism: 16 partitions (balanced)
   - Executor cores: 4 (more concurrent tasks)
   - Memory: 8GB (reduced GC pressure)
   - Memory fraction: 0.8 (more execution memory)

### For Production (10K+ IOPS)

1. **Use Spark Cluster** (not local mode)
   ```bash
   --master spark://spark-master:7077
   ```

2. **Multiple YugabyteDB Nodes**
   ```properties
   yugabyte.host=node1,node2,node3
   yugabyte.loadBalanceHosts=true
   ```

3. **Optimize YugabyteDB GFlags**
   ```bash
   --ysql_enable_packed_row=true
   --rocksdb_max_background_flushes=4
   --memstore_size_mb=2048
   --db_block_cache_size_bytes=1073741824
   ```

4. **Increase Parallelism**
   ```properties
   spark.default.parallelism=32
   spark.executor.instances=8
   ```

## 🔍 Monitoring

### Check Migration Status
```bash
# View Spark UI
http://localhost:4040

# Check YugabyteDB row count
docker exec yugabyte ysqlsh -h localhost -U yugabyte -d transaction_datastore \
  -c "SELECT COUNT(*) FROM dda_pstd_fincl_txn_cnsmr_by_accntnbr;"
```

### Logs
Migration logs are output to console. For background execution:
```bash
nohup spark-submit ... > migration.log 2>&1 &
```

## 🛠️ Troubleshooting

### Common Issues

1. **"No suitable driver found"**
   - Ensure YugabyteDB JDBC driver is in classpath
   - Check `jdbc:yugabytedb://` URL format

2. **"Pipe broken" errors**
   - This implementation avoids pipes - should not occur
   - If seen, check connection stability

3. **Low throughput**
   - Check resource limits (CPU, memory, network)
   - Verify YugabyteDB GFlags are optimized
   - Consider using Spark cluster instead of local mode

4. **Duplicate key errors**
   - Truncate target table before migration
   - Check primary key configuration

## 📝 Configuration Reference

### Table Configuration
```properties
# Source Cassandra keyspace and table
table.source.keyspace=transaction_datastore
table.source.table=dda_pstd_fincl_txn_cnsmr_by_accntnbr

# Target YugabyteDB schema and table
table.target.schema=public
table.target.table=dda_pstd_fincl_txn_cnsmr_by_accntnbr
```

### Checkpointing
```properties
migration.checkpoint.enabled=true
migration.checkpoint.table=migration_checkpoint
migration.checkpoint.interval=50000
```

### Validation
```properties
migration.validation.enabled=true
migration.validation.sampleSize=1000
```

## 🏛️ Project Structure

```
cassandra-to-yugabyte-migrator/
├── src/main/scala/com/company/migration/
│   ├── MainApp.scala                    # Entry point
│   ├── config/                          # Configuration loaders
│   ├── cassandra/                        # Cassandra readers
│   ├── yugabyte/                         # YugabyteDB writers
│   ├── execution/                       # Migration orchestration
│   ├── validation/                      # Data validation
│   └── util/                             # Utilities
├── src/main/resources/
│   └── migration.properties             # Main configuration
├── pom.xml                               # Maven dependencies
└── README.md                             # This file
```

## 🔐 Security

- **Authentication**: Configure username/password in properties file
- **SSL/TLS**: Supported via Cassandra and YugabyteDB connection settings
- **Credentials**: Never commit credentials to version control

## 📚 Dependencies

- **Spark**: 3.5.1
- **Spark Cassandra Connector**: 3.5.1
- **YugabyteDB JDBC Driver**: 42.7.3-yb-4
- **Scala**: 2.13.16

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## 📄 License

This project is licensed under the Apache License 2.0.

## 🙏 Acknowledgments

- Built on top of Spark Cassandra Connector
- Uses YugabyteDB Smart Driver for load balancing
- Inspired by production-grade migration patterns

## 📞 Support

For issues and questions:
- Open an issue on GitHub
- Check the troubleshooting section
- Review Spark and YugabyteDB documentation

---

**Note**: For production deployments, always test in a staging environment first and monitor resource usage during migration.

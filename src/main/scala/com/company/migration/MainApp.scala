package com.company.migration

import com.company.migration.config._
import com.company.migration.cassandra.SplitSizeDecider
import com.company.migration.execution.{CheckpointManager, TableMigrationJob}
import com.company.migration.util.{Logging, Metrics, ResourceUtils}
import com.company.migration.validation.{ChecksumValidator, RowCountValidator}
import com.company.migration.yugabyte.YugabyteConnectionFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

/**
 * Main entry point for the migration tool
 */
object MainApp extends Logging {
  
  def main(args: Array[String]): Unit = {
    val propertiesPath = if (args.length > 0) args(0) else "migration.properties"
    
    try {
      // Load configuration from properties file
      val props = ConfigLoader.load(propertiesPath)
      val cassandraConfig = CassandraConfig.fromProperties(props)
      val yugabyteConfig = YugabyteConfig.fromProperties(props)
      val sparkJobConfig = SparkJobConfig.fromProperties(props)
      
      // Migration settings
      val checkpointEnabled = props.getProperty("migration.checkpoint.enabled", "true").toBoolean
      val checkpointKeyspace = props.getProperty("migration.checkpoint.keyspace", "public")
      val checkpointInterval = props.getProperty("migration.checkpoint.interval", "10000").toInt
      
      // Run ID management
      val runIdStr = props.getProperty("migration.runId", "")
      val runId = if (runIdStr.nonEmpty) {
        runIdStr.toLong
      } else {
        System.currentTimeMillis() / 1000 // Auto-generate if not provided
      }
      
      val prevRunIdStr = props.getProperty("migration.prevRunId", "0")
      val prevRunId = if (prevRunIdStr.nonEmpty && prevRunIdStr != "0") {
        prevRunIdStr.toLong
      } else {
        0L
      }
      
      val validationEnabled = props.getProperty("migration.validation.enabled", "true").toBoolean
      val validationSampleSize = props.getProperty("migration.validation.sampleSize", "1000").toInt
      
      // Target table truncation (default: true to preserve current behavior)
      val truncateTargetTable = props.getProperty("yugabyte.truncateTargetTable", "true").toBoolean
      
      // Initialize Spark
      val sparkConf = createSparkConf(sparkJobConfig, cassandraConfig)
      val spark = SparkSession.builder()
        .config(sparkConf)
        .appName("Cassandra-to-YugabyteDB Migration")
        .getOrCreate()
      
      logInfo("Spark session created")
      
      // Initialize YugabyteDB connection factory FIRST
      // Driver is automatically loaded and registered in constructor (no pooling needed)
      val connectionFactory = new YugabyteConnectionFactory(yugabyteConfig)
      
      // Initialize checkpoint manager if enabled
      // Pattern: CheckpointManager creates its own connections per operation
      val checkpointManager = if (checkpointEnabled) {
        val cm = new CheckpointManager(yugabyteConfig, checkpointKeyspace)
        cm.initializeCheckpointTables()
        Some(cm)
      } else {
        None
      }
      
              // Initialize metrics (requires SparkContext for accumulators)
              val metrics = new Metrics(spark.sparkContext)
      
      try {
        // Check if table configuration exists
        if (!TableConfig.hasTableConfig(props)) {
          throw new IllegalArgumentException(
            "Table configuration not found. Please configure table.source.keyspace and table.source.table in properties file."
          )
        }
        
      // Load table configuration
      val tableConfig = TableConfig.fromProperties(props)
      logInfo(s"Migrating table: ${tableConfig.sourceKeyspace}.${tableConfig.sourceTable}")
      
      logInfo(s"========================================")
      logInfo(s"Migrating table: ${tableConfig.sourceKeyspace}.${tableConfig.sourceTable}")
      logInfo(s"========================================")
      
      // Determine optimal split size at runtime (BEFORE DataFrame read)
      val autoDetermine = props.getProperty("cassandra.inputSplitSizeMb.autoDetermine", "true").toBoolean
      val overrideSize = Option(props.getProperty("cassandra.inputSplitSizeMb.override"))
        .filter(_.nonEmpty)
        .map(_.toInt)
      
      val executorMemoryGb = {
        val memoryStr = sparkJobConfig.executorMemory.replaceAll("[^0-9]", "")
        if (memoryStr.nonEmpty) memoryStr.toInt / 1024 else 8 // Default to 8GB if can't parse
      }
      
      val optimalSplitSize = SplitSizeDecider.determineSplitSize(
        cassandraConfig,
        tableConfig,
        executorMemoryGb,
        autoDetermine,
        overrideSize
      )
      
      // Update SparkConf with optimal split size (must be done before DataFrame read)
      spark.conf.set("spark.cassandra.input.split.sizeInMB", optimalSplitSize.toString)
      spark.conf.set("cassandra.inputSplitSizeMb", optimalSplitSize.toString)
      
      // Update cassandraConfig for logging
      val updatedCassandraConfig = cassandraConfig.copy(inputSplitSizeMb = optimalSplitSize)
      
      logWarn(s"✅ Using optimal split size: ${optimalSplitSize}MB for migration")
      
      // Truncate target table before migration (optional)
      if (truncateTargetTable) {
        try {
          val truncateConn = connectionFactory.getConnection()
          try {
            val truncateStmt = truncateConn.createStatement()
            truncateStmt.execute(s"TRUNCATE TABLE ${tableConfig.targetSchema}.${tableConfig.targetTable}")
            truncateConn.commit()
            logInfo(s"Truncated target table: ${tableConfig.targetSchema}.${tableConfig.targetTable}")
          } finally {
            truncateConn.close()
          }
        } catch {
          case e: Exception =>
            logWarn(s"Could not truncate target table (may not exist or may be empty): ${e.getMessage}")
        }
      } else {
        logInfo(s"Skipping truncate for target table: ${tableConfig.targetSchema}.${tableConfig.targetTable}")
      }
      
      // Log run information
      if (checkpointEnabled) {
        logInfo(s"========================================")
        logInfo(s"Migration Run Information")
        logInfo(s"========================================")
        logInfo(s"Run ID: $runId")
        if (prevRunId > 0) {
          logInfo(s"Previous Run ID: $prevRunId (resume mode)")
        } else {
          logInfo(s"Previous Run ID: None (new run)")
        }
        logInfo(s"Table: ${tableConfig.sourceKeyspace}.${tableConfig.sourceTable}")
        logInfo(s"========================================")
      }
      
      // Create and execute migration job
      val migrationJob = new TableMigrationJob(
        spark,
        updatedCassandraConfig,
        yugabyteConfig,
        sparkJobConfig,
        tableConfig,
        connectionFactory,
        metrics,
        checkpointManager,
        runId,
        prevRunId,
        checkpointInterval
      )
        
        migrationJob.execute()
        
        // End checkpoint run if enabled
        checkpointManager.foreach { cm =>
          val runInfo = metrics.getSummary
          cm.endRun(
            tableName = s"${tableConfig.sourceKeyspace}.${tableConfig.sourceTable}",
            runId = runId,
            runInfo = runInfo
          )
        }
        
        // Validate if enabled
        // Pattern: Use migration metrics instead of COUNT queries (avoids timeout)
        if (validationEnabled && tableConfig.validate) {
          logWarn("Running validation using migration metrics (no COUNT queries)...")
          
          // Use metrics-based validation (no COUNT queries - avoids timeout on distributed DBs)
          val rowCountValidator = new RowCountValidator(metrics)
          val (rowsRead, rowsWritten, matchResult) = rowCountValidator.validateRowCount(tableConfig)
          
          if (!matchResult) {
            logWarn(s"Row count validation mismatch: Read=$rowsRead, Written=$rowsWritten")
            logWarn("  This may indicate write failures or skipped rows. Check error logs.")
          } else {
            logWarn(s"Row count validation passed: $rowsWritten rows migrated successfully")
          }
          
          // Optional: Checksum validation
          val checksumValidator = new ChecksumValidator(spark, cassandraConfig, yugabyteConfig, validationSampleSize)
          val checksumMatch = checksumValidator.validateChecksum(tableConfig)
          
          if (!checksumMatch) {
            logWarn("Checksum validation failed")
          } else {
            logInfo("Checksum validation passed")
          }
        }
        
        // Print final metrics
        logInfo("========================================")
        logInfo("Migration Summary")
        logInfo("========================================")
        logInfo(metrics.getSummary)
        
      } finally {
        // Cleanup
        connectionFactory.close()
        spark.stop()
        logInfo("Spark session stopped")
      }
      
    } catch {
      case e: Exception =>
        logError(s"Migration failed: ${e.getMessage}", e)
        System.exit(1)
    }
  }
  
  private def createSparkConf(
    sparkJobConfig: SparkJobConfig,
    cassandraConfig: CassandraConfig
  ): SparkConf = {
    val conf = new SparkConf()
      .set("spark.executor.instances", sparkJobConfig.executorInstances.toString)
      .set("spark.executor.cores", sparkJobConfig.executorCores.toString)
      .set("spark.executor.memory", sparkJobConfig.executorMemory)
      .set("spark.executor.memoryOverhead", sparkJobConfig.executorMemoryOverhead)
      .set("spark.driver.memory", sparkJobConfig.driverMemory)
      .set("spark.default.parallelism", sparkJobConfig.defaultParallelism.toString)
      .set("spark.sql.shuffle.partitions", sparkJobConfig.shufflePartitions.toString)
      .set("spark.memory.fraction", sparkJobConfig.memoryFraction.toString)
      .set("spark.memory.storageFraction", sparkJobConfig.storageFraction.toString)
      .set("spark.task.maxFailures", sparkJobConfig.taskMaxFailures.toString)
      .set("spark.stage.maxConsecutiveAttempts", sparkJobConfig.stageMaxConsecutiveAttempts.toString)
      .set("spark.network.timeout", sparkJobConfig.networkTimeout)
      .set("spark.serializer", sparkJobConfig.serializer)
              // Cassandra connector settings
      .set("spark.cassandra.connection.host", cassandraConfig.hosts)
      .set("spark.cassandra.connection.port", cassandraConfig.port.toString)
      .set("spark.cassandra.connection.local_dc", cassandraConfig.localDC) // Note: local_dc (underscore) not localDC
      .set("spark.cassandra.read.timeoutMS", cassandraConfig.readTimeoutMs.toString)
      .set("spark.cassandra.input.fetch.sizeInRows", cassandraConfig.fetchSizeInRows.toString)
      .set("spark.cassandra.input.split.sizeInMB", cassandraConfig.inputSplitSizeMb.toString)
      .set("spark.cassandra.input.consistency.level", cassandraConfig.consistencyLevel)
      .set("spark.cassandra.concurrent.reads", cassandraConfig.concurrentReads.toString)
              // Advanced connection settings - only set if connector supports them
      // Note: Some properties may not be supported by all connector versions
      // These are optional and will be ignored if not supported
      try {
        conf.set("spark.cassandra.connection.localConnectionsPerExecutor", cassandraConfig.localConnectionsPerExecutor.toString)
        conf.set("spark.cassandra.connection.remoteConnectionsPerExecutor", cassandraConfig.remoteConnectionsPerExecutor.toString)
        conf.set("spark.cassandra.connection.keep_alive_ms", cassandraConfig.keepAliveMs.toString)
        conf.set("spark.cassandra.connection.reconnection_delay_ms.min", cassandraConfig.reconnectionDelayMsMin.toString)
        conf.set("spark.cassandra.connection.reconnection_delay_ms.max", cassandraConfig.reconnectionDelayMsMax.toString)
        conf.set("spark.cassandra.connection.factory", cassandraConfig.connectionFactory)
      } catch {
        case _: Exception => // Ignore if properties not supported
      }
    
    logInfo(s"Cassandra connection configured: host=${cassandraConfig.hosts}, port=${cassandraConfig.port}, localDC=${cassandraConfig.localDC}")
    
    // Add authentication if provided
    cassandraConfig.username.foreach { username =>
      conf.set("spark.cassandra.auth.username", username)
      cassandraConfig.password.foreach { password =>
        conf.set("spark.cassandra.auth.password", password)
      }
    }
    
    // Dynamic allocation
    if (sparkJobConfig.dynamicAllocationEnabled) {
      conf.set("spark.dynamicAllocation.enabled", "true")
        .set("spark.dynamicAllocation.minExecutors", sparkJobConfig.dynamicAllocationMinExecutors.toString)
        .set("spark.dynamicAllocation.maxExecutors", sparkJobConfig.dynamicAllocationMaxExecutors.toString)
        .set("spark.dynamicAllocation.initialExecutors", sparkJobConfig.dynamicAllocationInitialExecutors.toString)
    }
    
    conf
  }
}

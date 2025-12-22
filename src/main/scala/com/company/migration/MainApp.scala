package com.company.migration

import com.company.migration.config._
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
      val checkpointTable = props.getProperty("migration.checkpoint.table", "migration_checkpoint")
      val checkpointInterval = props.getProperty("migration.checkpoint.interval", "10000").toInt
      val jobId = props.getProperty("migration.jobId", s"migration-job-${System.currentTimeMillis() / 1000}")
      val validationEnabled = props.getProperty("migration.validation.enabled", "true").toBoolean
      val validationSampleSize = props.getProperty("migration.validation.sampleSize", "1000").toInt
      
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
      // CDM Pattern: CheckpointManager creates its own connections per operation
      val checkpointManager = if (checkpointEnabled) {
        val cm = new CheckpointManager(yugabyteConfig, checkpointTable)
        cm.initializeCheckpointTable()
        Some(cm)
      } else {
        None
      }
      
      // Initialize metrics
      val metrics = new Metrics()
      
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
        
        // Create and execute migration job
        val migrationJob = new TableMigrationJob(
          spark,
          cassandraConfig,
          yugabyteConfig,
          sparkJobConfig,
          tableConfig,
          connectionFactory,
          metrics,
          checkpointManager,
          Some(jobId),
          checkpointInterval
        )
        
        migrationJob.execute()
        
        // Validate if enabled
        if (validationEnabled && tableConfig.validate) {
          logInfo("Running validation...")
          
          val rowCountValidator = new RowCountValidator(spark, cassandraConfig, yugabyteConfig)
          val (cassandraCount, yugabyteCount, matchResult) = rowCountValidator.validateRowCount(tableConfig)
          
          if (!matchResult) {
            logWarn(s"Row count mismatch: Cassandra=$cassandraCount, Yugabyte=$yugabyteCount")
          } else {
            logInfo("Row count validation passed")
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
      // Cassandra connector settings (CDM-style)
      .set("spark.cassandra.connection.host", cassandraConfig.hosts)
      .set("spark.cassandra.connection.port", cassandraConfig.port.toString)
      .set("spark.cassandra.connection.local_dc", cassandraConfig.localDC) // Note: local_dc (underscore) not localDC
      .set("spark.cassandra.read.timeoutMS", cassandraConfig.readTimeoutMs.toString)
      .set("spark.cassandra.input.fetch.sizeInRows", cassandraConfig.fetchSizeInRows.toString)
      .set("spark.cassandra.input.split.sizeInMB", cassandraConfig.inputSplitSizeMb.toString)
      .set("spark.cassandra.input.consistency.level", cassandraConfig.consistencyLevel)
      .set("spark.cassandra.concurrent.reads", cassandraConfig.concurrentReads.toString)
      // Advanced connection settings (CDM-style) - only set if connector supports them
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

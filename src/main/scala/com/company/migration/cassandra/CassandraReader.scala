package com.company.migration.cassandra

import com.company.migration.config.CassandraConfig
import com.company.migration.config.TableConfig
import com.company.migration.util.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Reads data from Cassandra using Spark Cassandra Connector
 * Leverages token-aware partitioning for optimal performance
 */
class CassandraReader(spark: SparkSession, cassandraConfig: CassandraConfig) extends Logging {
  
  /**
   * Read a table from Cassandra into a Spark DataFrame
   * Uses token-aware partitioning for optimal parallelism
   */
  def readTable(tableConfig: TableConfig): DataFrame = {
    logInfo(s"Reading table: ${tableConfig.sourceKeyspace}.${tableConfig.sourceTable}")
    
    // DataFrameReader.options() only accepts keyspace/table and auth options
    // All other spark.cassandra.* configs must be set in SparkConf (done in MainApp)
    val df = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map(
        "keyspace" -> tableConfig.sourceKeyspace,
        "table" -> tableConfig.sourceTable
      ) ++ (if (cassandraConfig.username.isDefined) {
        Map(
          "spark.cassandra.auth.username" -> cassandraConfig.username.get,
          "spark.cassandra.auth.password" -> cassandraConfig.password.getOrElse("")
        )
      } else Map.empty[String, String]))
      .load()
    
    logInfo(s"Successfully read table: ${tableConfig.sourceKeyspace}.${tableConfig.sourceTable}")
    logInfo(s"Partitions: ${df.rdd.getNumPartitions}")
    // NOTE: Removed df.count() to avoid triggering full table scan during planning phase
    // Row count will be tracked via Metrics during actual migration
    
    df
  }
}


package com.company.migration.cassandra

import com.company.migration.util.Logging
import org.apache.spark.sql.DataFrame

/**
 * Utilities for token range partitioning
 * Spark Cassandra Connector handles this automatically, but this provides
 * additional utilities for monitoring and optimization
 */
object CassandraTokenPartitioner extends Logging {
  
  /**
   * Get partition count for a DataFrame
   * This reflects the token range splits
   */
  def getPartitionCount(df: DataFrame): Int = {
    df.rdd.getNumPartitions
  }
  
  /**
   * Log partition information for debugging
   */
  def logPartitionInfo(df: DataFrame): Unit = {
    val partitionCount = getPartitionCount(df)
    logInfo(s"DataFrame has $partitionCount partitions (token ranges)")
    
    // Get approximate row counts per partition
    val partitionSizes = df.rdd.mapPartitions(iter => Iterator(iter.size)).collect()
    if (partitionSizes.nonEmpty) {
      val min = partitionSizes.min
      val max = partitionSizes.max
      val avg = partitionSizes.sum / partitionSizes.length
      logInfo(s"Partition sizes - Min: $min, Max: $max, Avg: $avg")
    }
  }
  
  /**
   * Repartition if needed to balance load
   * Generally not needed as Spark Cassandra Connector handles this well
   */
  def repartitionIfNeeded(df: DataFrame, targetPartitions: Int): DataFrame = {
    val currentPartitions = getPartitionCount(df)
    if (currentPartitions != targetPartitions) {
      logInfo(s"Repartitioning from $currentPartitions to $targetPartitions partitions")
      df.repartition(targetPartitions)
    } else {
      df
    }
  }
}


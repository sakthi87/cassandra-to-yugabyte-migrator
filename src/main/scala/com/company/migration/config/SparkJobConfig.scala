package com.company.migration.config

import java.util.Properties

/**
 * Spark job configuration
 */
case class SparkJobConfig(
  executorInstances: Int,
  executorCores: Int,
  executorMemory: String,
  executorMemoryOverhead: String,
  driverMemory: String,
  defaultParallelism: Int,
  shufflePartitions: Int,
  memoryFraction: Double,
  storageFraction: Double,
  taskMaxFailures: Int,
  stageMaxConsecutiveAttempts: Int,
  networkTimeout: String,
  serializer: String,
  dynamicAllocationEnabled: Boolean,
  dynamicAllocationMinExecutors: Int,
  dynamicAllocationMaxExecutors: Int,
  dynamicAllocationInitialExecutors: Int
)

object SparkJobConfig {
  def fromProperties(props: Properties): SparkJobConfig = {
    def getProperty(key: String, default: String = ""): String = {
      props.getProperty(key, default)
    }
    
    def getIntProperty(key: String, default: Int): Int = {
      val value = props.getProperty(key)
      if (value != null && value.nonEmpty) value.toInt else default
    }
    
    def getDoubleProperty(key: String, default: Double): Double = {
      val value = props.getProperty(key)
      if (value != null && value.nonEmpty) value.toDouble else default
    }
    
    def getBooleanProperty(key: String, default: Boolean): Boolean = {
      val value = props.getProperty(key)
      if (value != null && value.nonEmpty) value.toBoolean else default
    }
    
    SparkJobConfig(
      executorInstances = getIntProperty("spark.executor.instances", 6),
      executorCores = getIntProperty("spark.executor.cores", 2),
      executorMemory = getProperty("spark.executor.memory", "4g"),
      executorMemoryOverhead = getProperty("spark.executor.memoryOverhead", "1024m"),
      driverMemory = getProperty("spark.driver.memory", "4g"),
      defaultParallelism = getIntProperty("spark.default.parallelism", 12),
      shufflePartitions = getIntProperty("spark.sql.shuffle.partitions", 12),
      memoryFraction = getDoubleProperty("spark.memory.fraction", 0.6),
      storageFraction = getDoubleProperty("spark.memory.storageFraction", 0.3),
      taskMaxFailures = getIntProperty("spark.task.maxFailures", 10),
      stageMaxConsecutiveAttempts = getIntProperty("spark.stage.maxConsecutiveAttempts", 4),
      networkTimeout = getProperty("spark.network.timeout", "800s"),
      serializer = getProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
      dynamicAllocationEnabled = getBooleanProperty("spark.dynamicAllocation.enabled", false),
      dynamicAllocationMinExecutors = getIntProperty("spark.dynamicAllocation.minExecutors", 6),
      dynamicAllocationMaxExecutors = getIntProperty("spark.dynamicAllocation.maxExecutors", 12),
      dynamicAllocationInitialExecutors = getIntProperty("spark.dynamicAllocation.initialExecutors", 6)
    )
  }
}

package com.company.migration.config

import java.util.Properties

/**
 * YugabyteDB connection and COPY configuration
 */
case class YugabyteConfig(
  hosts: List[String],  // List of hosts for round-robin load balancing
  port: Int,
  database: String,
  username: String,
  password: String,
  maxPoolSize: Int,
  minIdle: Int,
  connectionTimeout: Int,
  idleTimeout: Int,
  maxLifetime: Int,
  loadBalanceHosts: Boolean,
  copyBufferSize: Int,
  copyFlushEvery: Int,
  csvDelimiter: String,
  csvNull: String,
  csvQuote: String,
  csvEscape: String,
  isolationLevel: String,
  autoCommit: Boolean,
  jdbcParams: String,  // JDBC URL parameters (without host/port/database)
  insertMode: String,  // "COPY" or "INSERT" - controls which insert method to use
  insertBatchSize: Int,  // Batch size for INSERT mode (default: 1000)
  disableTransactionalWrites: Boolean,  // Set yb_disable_transactional_writes = on for performance (default: false)
  copyReplace: Boolean  // Use COPY WITH REPLACE for idempotent upsert behavior (default: false)
) {
  /**
   * Get base JDBC URL for a single host (used for round-robin connection)
   */
  def getJdbcUrlForHost(host: String): String = {
    val baseUrl = s"jdbc:yugabytedb://$host:$port/$database"
    if (jdbcParams.nonEmpty) s"$baseUrl?$jdbcParams" else baseUrl
  }
  
  /**
   * Legacy jdbcUrl field for backward compatibility (uses first host)
   * @deprecated Use getJdbcUrlForHost() for round-robin connections
   */
  @deprecated("Use getJdbcUrlForHost() for proper load balancing", "1.0")
  def jdbcUrl: String = getJdbcUrlForHost(hosts.head)
}

object YugabyteConfig {
  def fromProperties(props: Properties): YugabyteConfig = {
    def getProperty(key: String, default: String = ""): String = {
      props.getProperty(key, default)
    }
    
    def getIntProperty(key: String, default: Int): Int = {
      val value = props.getProperty(key)
      if (value != null && value.nonEmpty) value.toInt else default
    }
    
    def getBooleanProperty(key: String, default: Boolean): Boolean = {
      val value = props.getProperty(key)
      if (value != null && value.nonEmpty) value.toBoolean else default
    }
    
    val host = getProperty("yugabyte.host", "localhost")
    val port = getIntProperty("yugabyte.port", 5433)
    val database = getProperty("yugabyte.database", "yugabyte")
    
    // Parse hosts (support comma-separated list for multi-node clusters)
    val hosts = host.split(",").map(_.trim).filter(_.nonEmpty).toList
    
    // Build JDBC URL parameters (without host/port/database)
    val jdbcParams = buildJdbcParams(props)
    
    YugabyteConfig(
      hosts = hosts,
      port = port,
      database = database,
      username = getProperty("yugabyte.username", "yugabyte"),
      password = getProperty("yugabyte.password", "yugabyte"),
      maxPoolSize = getIntProperty("yugabyte.maxPoolSize", 8),
      minIdle = getIntProperty("yugabyte.minIdle", 2),
      connectionTimeout = getIntProperty("yugabyte.connectionTimeout", 30000),
      idleTimeout = getIntProperty("yugabyte.idleTimeout", 300000),
      maxLifetime = getIntProperty("yugabyte.maxLifetime", 1800000),
      loadBalanceHosts = getBooleanProperty("yugabyte.loadBalanceHosts", true),
      copyBufferSize = getIntProperty("yugabyte.copyBufferSize", 10000),
      copyFlushEvery = getIntProperty("yugabyte.copyFlushEvery", 10000),
      csvDelimiter = getProperty("yugabyte.csvDelimiter", ","),
      csvNull = getProperty("yugabyte.csvNull", ""),
      csvQuote = getProperty("yugabyte.csvQuote", "\""),
      csvEscape = getProperty("yugabyte.csvEscape", "\""),
      isolationLevel = getProperty("yugabyte.isolationLevel", "READ_COMMITTED"),
      autoCommit = getBooleanProperty("yugabyte.autoCommit", false),
      jdbcParams = jdbcParams,
      insertMode = getProperty("yugabyte.insertMode", "COPY").toUpperCase,  // COPY or INSERT
      insertBatchSize = getIntProperty("yugabyte.insertBatchSize", 1000),  // Batch size for INSERT mode
      disableTransactionalWrites = getBooleanProperty("yugabyte.disableTransactionalWrites", false),  // Enable yb_disable_transactional_writes for performance
      copyReplace = getBooleanProperty("yugabyte.copyReplace", false)  // Use COPY WITH REPLACE for idempotent upsert (default: false)
    )
  }
  
  private def buildJdbcParams(props: Properties): String = {
    val params = scala.collection.mutable.ListBuffer[String]()
    
    // Note: We don't use loadBalance=true in JDBC params when doing round-robin
    // Round-robin selection happens at connection factory level
    // Only add topologyKeys if explicitly configured (for multi-region scenarios)
    val topologyKeys = props.getProperty("yugabyte.topologyKeys", "")
    if (topologyKeys.nonEmpty) {
      params += s"topologyKeys=$topologyKeys"
    }
    
    // COPY-optimized properties (mandatory for performance)
    params += "preferQueryMode=simple"  // Avoids server-side prepare overhead
    params += "binaryTransfer=false"    // COPY text mode is faster & safer
    params += "stringtype=unspecified"  // Avoids text cast overhead
    params += "reWriteBatchedInserts=true"  // Required even if using COPY
    
    // Timeouts (critical for COPY streams)
    params += "connectTimeout=10"
    params += "socketTimeout=0"  // Critical: COPY streams can run minutes
    params += "loginTimeout=10"
    
    // Connection keep-alive
    params += "tcpKeepAlive=true"
    params += "keepAlive=true"
    
    params.mkString("&")
  }
}

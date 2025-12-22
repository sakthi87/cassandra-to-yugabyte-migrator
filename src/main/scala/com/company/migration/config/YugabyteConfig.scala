package com.company.migration.config

import java.util.Properties

/**
 * YugabyteDB connection and COPY configuration
 */
case class YugabyteConfig(
  jdbcUrl: String,
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
  autoCommit: Boolean
)

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
    
    // Build JDBC URL - Always use YugabyteDB format (jdbc:yugabytedb://)
    // YugabyteDB driver requires jdbc:yugabytedb:// URL format
    val hosts = host.split(",").map(_.trim)
    val baseUrl = if (hosts.length > 1) {
      // Multiple hosts - use YugabyteDB format for load balancing
      val hostPorts = hosts.map(h => s"$h:$port").mkString(",")
      s"jdbc:yugabytedb://$hostPorts/$database"
    } else {
      // Single host - use YugabyteDB format (required for YugabyteDB driver)
      s"jdbc:yugabytedb://$host:$port/$database"
    }
    val urlParams = buildJdbcParams(props)
    val jdbcUrl = if (urlParams.nonEmpty) s"$baseUrl?$urlParams" else baseUrl
    
    YugabyteConfig(
      jdbcUrl = jdbcUrl,
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
      autoCommit = getBooleanProperty("yugabyte.autoCommit", false)
    )
  }
  
  private def buildJdbcParams(props: Properties): String = {
    val params = scala.collection.mutable.ListBuffer[String]()
    
    // Load balancing (critical for multi-node)
    val loadBalance = props.getProperty("yugabyte.loadBalanceHosts", "true").toBoolean
    if (loadBalance) {
      params += "loadBalance=true"
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
    
    // Topology keys (optional, for stretch clusters)
    val topologyKeys = props.getProperty("yugabyte.topologyKeys", "")
    if (topologyKeys.nonEmpty) {
      params += s"topologyKeys=$topologyKeys"
    }
    
    params.mkString("&")
  }
}

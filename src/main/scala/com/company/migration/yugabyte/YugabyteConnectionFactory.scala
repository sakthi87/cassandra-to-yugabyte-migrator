package com.company.migration.yugabyte

import com.company.migration.config.YugabyteConfig
import com.company.migration.util.Logging
import java.sql.{Connection, DriverManager, SQLException}
import java.util.concurrent.atomic.AtomicInteger

/**
 * Factory for creating YugabyteDB connections with round-robin load balancing
 * 
 * CRITICAL: Uses DriverManager.getConnection() per Spark partition (NOT HikariCP)
 * 
 * Load Balancing:
 * - Implements round-robin host selection to distribute connections across nodes
 * - Each partition gets a connection to a different host (distributed evenly)
 * - This balances CPU load across all YugabyteDB nodes
 * 
 * Why no pooling?
 * - COPY FROM STDIN is long-lived (minutes per connection)
 * - Spark already parallelizes at partition level
 * - COPY streams are not multiplexable (one stream = one connection)
 * - Pooling causes classloader issues in Spark
 * - Pooling can cause broken pipe errors when connections are evicted mid-COPY
 * 
 * This matches production patterns used by:
 * - Databricks bulk loaders
 * - Yugabyte internal loaders  
 * - Postgres COPY tools
 */
class YugabyteConnectionFactory(yugabyteConfig: YugabyteConfig) extends Logging {
  
  // Store driver instance to use directly (avoids DriverManager classloader issues)
  private val driver: java.sql.Driver = initDriver()
  
  // Note: Round-robin is now based on partition ID (passed to getConnection)
  // This ensures proper distribution across Spark partitions without shared state
  
  private def initDriver(): java.sql.Driver = {
    val classLoader = Thread.currentThread().getContextClassLoader
    try {
      // Load YugabyteDB driver only (required)
      // Pattern: Load driver using context classloader (critical for Spark)
      val contextClassLoader = if (classLoader != null) classLoader else this.getClass.getClassLoader
      val driverInstance = Class
        .forName("com.yugabyte.Driver", true, contextClassLoader)
        .getDeclaredConstructor()
        .newInstance()
        .asInstanceOf[java.sql.Driver]
      
      // Pattern: Register driver with DriverManager (CRITICAL!)
      // This ensures proper driver initialization and URL acceptance
      DriverManager.registerDriver(driverInstance)
      
      logInfo("YugabyteDB JDBC Driver loaded and registered successfully")
      driverInstance
    } catch {
      case e: ClassNotFoundException =>
        logError("Failed to load YugabyteDB JDBC driver", e)
        throw new RuntimeException(
          "YugabyteDB JDBC driver (com.yugabyte.Driver) not found. " +
          "Make sure jdbc-yugabytedb dependency is in classpath. " +
          "Add to pom.xml: <dependency><groupId>com.yugabyte</groupId><artifactId>jdbc-yugabytedb</artifactId></dependency>", e)
      case e: Exception =>
        logError("Failed to initialize YugabyteDB JDBC driver", e)
        throw new RuntimeException("Failed to initialize YugabyteDB JDBC driver", e)
    }
  }
  
  /**
   * Get a connection for a Spark partition with round-robin load balancing
   * 
   * CRITICAL: One connection per Spark partition (not pooled)
   * This connection should be used for the entire COPY operation
   * and closed after the partition is processed.
   * 
   * Load Balancing:
   * - Uses partition ID to deterministically select host
   * - Partition 0 → host[0], Partition 1 → host[1], Partition 2 → host[2], Partition 3 → host[0] (wraps)
   * - Ensures even distribution of COPY connections across YugabyteDB nodes
   * 
   * @param partitionId Spark partition ID (0, 1, 2, ...)
   */
  def getConnection(partitionId: Int = 0): Connection = {
    val props = new java.util.Properties()
    props.setProperty("user", yugabyteConfig.username)
    props.setProperty("password", yugabyteConfig.password)
    
    // COPY-optimized properties (mandatory for performance)
    props.setProperty("preferQueryMode", "simple")  // Avoids server-side prepare overhead
    props.setProperty("binaryTransfer", "false")    // COPY text mode is faster & safer
    props.setProperty("stringtype", "unspecified")  // Avoids text cast overhead
    props.setProperty("socketTimeout", "0")        // Critical: COPY streams can run minutes
    props.setProperty("tcpKeepAlive", "true")
    props.setProperty("keepAlive", "true")
    
    // Additional COPY-optimized properties
    props.setProperty("reWriteBatchedInserts", "true")
    props.setProperty("connectTimeout", "10")
    props.setProperty("loginTimeout", "10")
    
    // Round-robin host selection based on partition ID for load balancing
    val hosts = yugabyteConfig.hosts
    if (hosts.isEmpty) {
      throw new SQLException("No YugabyteDB hosts configured. Check yugabyte.host property.")
    }
    
    // Use partition ID to deterministically select host (works perfectly in Spark)
    // Partition 0 → host[0], Partition 1 → host[1], Partition 2 → host[2], etc.
    val hostIndex = partitionId % hosts.length
    val selectedHost = hosts(hostIndex)
    
    // Build JDBC URL for the selected host
    val jdbcUrl = yugabyteConfig.getJdbcUrlForHost(selectedHost)
    
    logInfo(s"Connecting to YugabyteDB host $selectedHost (${hostIndex + 1}/${hosts.length}) for partition $partitionId")
    
    // Pattern: Verify driver accepts URL before connecting
    if (!driver.acceptsURL(jdbcUrl)) {
      throw new SQLException(s"YugabyteDB driver does not accept URL: $jdbcUrl. Ensure URL uses jdbc:yugabytedb:// format.")
    }
    
    // Use DriverManager.getConnection() - driver is pre-registered
    // This matches production pattern and works correctly with Spark classloader
    val conn = DriverManager.getConnection(jdbcUrl, props)
    
    // Set transaction isolation and auto-commit
    conn.setTransactionIsolation(getIsolationLevel(yugabyteConfig.isolationLevel))
    conn.setAutoCommit(yugabyteConfig.autoCommit)
    
    logDebug(s"Created new connection to $selectedHost for partition: ${conn.getClass.getSimpleName}")
    conn
  }
  
  /**
   * Get a direct connection (for testing or single-use)
   * Same as getConnection() but explicitly documented
   */
  def getDirectConnection(): Connection = {
    getConnection(0)  // Default partition ID 0 for non-Spark usage
  }
  
  /**
   * Close method (no-op since we don't use pooling)
   * Connections are closed by callers after use
   */
  def close(): Unit = {
    logDebug("YugabyteConnectionFactory.close() called (no-op, no pooling)")
  }
  
  private def getIsolationLevel(level: String): Int = {
    level.toUpperCase match {
      case "READ_COMMITTED" => Connection.TRANSACTION_READ_COMMITTED
      case "REPEATABLE_READ" => Connection.TRANSACTION_REPEATABLE_READ
      case "SERIALIZABLE" => Connection.TRANSACTION_SERIALIZABLE
      case _ => Connection.TRANSACTION_READ_COMMITTED
    }
  }
}

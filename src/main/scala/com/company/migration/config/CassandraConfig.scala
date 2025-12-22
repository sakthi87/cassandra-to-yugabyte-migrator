package com.company.migration.config

import java.util.Properties

/**
 * Cassandra connection and read configuration
 */
case class CassandraConfig(
  hosts: String,
  port: Int,
  localDC: String,
  username: Option[String],
  password: Option[String],
  readTimeoutMs: Int,
  fetchSizeInRows: Int,
  consistencyLevel: String,
  retryCount: Int,
  inputSplitSizeMb: Int,
  concurrentReads: Int,
  readsPerSec: Int,
  // Advanced connection settings (CDM-style)
  localConnectionsPerExecutor: Int,
  remoteConnectionsPerExecutor: Int,
  connectionTimeoutMs: Int,
  keepAliveMs: Int,
  reconnectionDelayMsMin: Int,
  reconnectionDelayMsMax: Int,
  maxRequestsPerConnectionLocal: Int,
  maxRequestsPerConnectionRemote: Int,
  connectionFactory: String
)

object CassandraConfig {
  def fromProperties(props: Properties): CassandraConfig = {
    def getProperty(key: String, default: String = ""): String = {
      props.getProperty(key, default)
    }
    
    def getIntProperty(key: String, default: Int): Int = {
      val value = props.getProperty(key)
      if (value != null && value.nonEmpty) value.toInt else default
    }
    
    CassandraConfig(
      hosts = getProperty("cassandra.host"),
      port = getIntProperty("cassandra.port", 9042),
      localDC = getProperty("cassandra.localDC", "datacenter1"),
      username = {
        val user = getProperty("cassandra.username")
        if (user.nonEmpty) Some(user) else None
      },
      password = {
        val pass = getProperty("cassandra.password")
        if (pass.nonEmpty) Some(pass) else None
      },
      readTimeoutMs = getIntProperty("cassandra.readTimeoutMs", 120000),
      fetchSizeInRows = getIntProperty("cassandra.fetchSizeInRows", 1000),
      consistencyLevel = getProperty("cassandra.consistencyLevel", "LOCAL_QUORUM"),
      retryCount = getIntProperty("cassandra.retryCount", 3),
      inputSplitSizeMb = getIntProperty("cassandra.inputSplitSizeMb", 64),
      concurrentReads = getIntProperty("cassandra.concurrentReads", 512),
      readsPerSec = getIntProperty("cassandra.readsPerSec", 0),
      // Advanced connection settings
      localConnectionsPerExecutor = getIntProperty("cassandra.connection.localConnectionsPerExecutor", 4),
      remoteConnectionsPerExecutor = getIntProperty("cassandra.connection.remoteConnectionsPerExecutor", 1),
      connectionTimeoutMs = getIntProperty("cassandra.connection.timeoutMs", 60000),
      keepAliveMs = getIntProperty("cassandra.connection.keepAliveMs", 30000),
      reconnectionDelayMsMin = getIntProperty("cassandra.connection.reconnectionDelayMs.min", 1000),
      reconnectionDelayMsMax = getIntProperty("cassandra.connection.reconnectionDelayMs.max", 60000),
      maxRequestsPerConnectionLocal = getIntProperty("cassandra.connection.maxRequestsPerConnection.local", 32768),
      maxRequestsPerConnectionRemote = getIntProperty("cassandra.connection.maxRequestsPerConnection.remote", 2000),
      connectionFactory = getProperty("cassandra.connection.factory", "com.datastax.spark.connector.cql.DefaultConnectionFactory")
    )
  }
}

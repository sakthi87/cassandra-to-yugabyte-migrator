package com.company.migration.validation

import com.company.migration.config.{CassandraConfig, TableConfig, YugabyteConfig}
import com.company.migration.util.Logging
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.SparkSession

import java.sql.{Connection, DriverManager}

/**
 * Validates row counts between Cassandra and YugabyteDB
 */
class RowCountValidator(
  spark: SparkSession,
  cassandraConfig: CassandraConfig,
  yugabyteConfig: YugabyteConfig
) extends Logging {
  
  /**
   * Validate row counts for a table
   * @return (cassandraCount, yugabyteCount, match)
   */
  def validateRowCount(tableConfig: TableConfig): (Long, Long, Boolean) = {
    logInfo(s"Validating row counts for ${tableConfig.sourceKeyspace}.${tableConfig.sourceTable}")
    
    val cassandraCount = getCassandraRowCount(tableConfig)
    val yugabyteCount = getYugabyteRowCount(tableConfig)
    val matchResult = cassandraCount == yugabyteCount
    
    logInfo(s"Row count validation: Cassandra=$cassandraCount, Yugabyte=$yugabyteCount, Match=$matchResult")
    
    (cassandraCount, yugabyteCount, matchResult)
  }
  
  private def getCassandraRowCount(tableConfig: TableConfig): Long = {
    try {
      val connector = CassandraConnector(
        spark.sparkContext.getConf.set("spark.cassandra.connection.host", cassandraConfig.hosts)
          .set("spark.cassandra.connection.port", cassandraConfig.port.toString)
      )
      
      connector.withSessionDo { session =>
        val query = s"SELECT COUNT(*) FROM ${tableConfig.sourceKeyspace}.${tableConfig.sourceTable}"
        val result = session.execute(query)
        result.one().getLong(0)
      }
    } catch {
      case e: Exception =>
        logError(s"Error getting Cassandra row count: ${e.getMessage}", e)
        throw e
    }
  }
  
  private def getYugabyteRowCount(tableConfig: TableConfig): Long = {
    var conn: Option[Connection] = None
    try {
      conn = Some(DriverManager.getConnection(
        yugabyteConfig.jdbcUrl,
        yugabyteConfig.username,
        yugabyteConfig.password
      ))
      
      val query = s"SELECT COUNT(*) FROM ${tableConfig.targetSchema}.${tableConfig.targetTable}"
      val stmt = conn.get.createStatement()
      val rs = stmt.executeQuery(query)
      
      if (rs.next()) {
        rs.getLong(1)
      } else {
        0L
      }
    } catch {
      case e: Exception =>
        logError(s"Error getting Yugabyte row count: ${e.getMessage}", e)
        throw e
    } finally {
      conn.foreach(_.close())
    }
  }
}


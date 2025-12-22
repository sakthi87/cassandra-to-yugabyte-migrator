package com.company.migration.execution

import com.company.migration.config.YugabyteConfig
import com.company.migration.util.Logging
import com.company.migration.yugabyte.YugabyteConnectionFactory
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Timestamp}
import java.time.Instant

/**
 * Manages checkpointing for migration jobs
 * Based on CDM's TrackRun concept but adapted for Spark partitions
 */
/**
 * Manages checkpointing for migration jobs
 * Based on CDM's TrackRun concept but adapted for Spark partitions
 * 
 * CDM Pattern: Creates connections per operation (not stored)
 * This ensures serializability and follows CDM's design
 */
class CheckpointManager(
  val yugabyteConfig: YugabyteConfig,
  val checkpointTable: String
) extends Logging with Serializable {
  
  /**
   * Initialize checkpoint table if it doesn't exist
   */
  def initializeCheckpointTable(): Unit = {
    // Recreate connection factory per operation (CDM pattern)
    val connectionFactory = new YugabyteConnectionFactory(yugabyteConfig)
    var conn: Option[Connection] = None
    try {
      conn = Some(connectionFactory.getConnection())
      
      val createTableSql =
        s"""
           |CREATE TABLE IF NOT EXISTS $checkpointTable (
           |    job_id          TEXT,
           |    token_start     BIGINT,
           |    token_end       BIGINT,
           |    partition_id    INT,
           |    last_pk         TEXT,
           |    rows_written    BIGINT,
           |    status          TEXT,
           |    updated_at      TIMESTAMPTZ DEFAULT now(),
           |    PRIMARY KEY (job_id, partition_id)
           |);
           |""".stripMargin
      
      val stmt = conn.get.createStatement()
      stmt.execute(createTableSql)
      logInfo(s"Checkpoint table '$checkpointTable' initialized")
      
    } catch {
      case e: Exception =>
        logError(s"Error initializing checkpoint table: ${e.getMessage}", e)
        throw e
    } finally {
      conn.foreach(_.close())
    }
  }
  
  /**
   * Initialize a checkpoint for a partition
   */
  def initCheckpoint(jobId: String, partitionId: Int, tokenStart: Long, tokenEnd: Long): Unit = {
    // Recreate connection factory per operation (CDM pattern)
    val connectionFactory = new YugabyteConnectionFactory(yugabyteConfig)
    var conn: Option[Connection] = None
    try {
      conn = Some(connectionFactory.getConnection())
      
      val sql =
        s"""
           |INSERT INTO $checkpointTable 
           |(job_id, partition_id, token_start, token_end, status, rows_written, updated_at)
           |VALUES (?, ?, ?, ?, 'PENDING', 0, now())
           |ON CONFLICT (job_id, partition_id) DO NOTHING
           |""".stripMargin
      
      val stmt = conn.get.prepareStatement(sql)
      stmt.setString(1, jobId)
      stmt.setInt(2, partitionId)
      stmt.setLong(3, tokenStart)
      stmt.setLong(4, tokenEnd)
      stmt.executeUpdate()
      
    } catch {
      case e: Exception =>
        logError(s"Error initializing checkpoint: ${e.getMessage}", e)
        throw e
    } finally {
      conn.foreach(_.close())
    }
  }
  
  /**
   * Update checkpoint status
   */
  def updateCheckpoint(
    jobId: String,
    partitionId: Int,
    status: String,
    rowsWritten: Long = 0,
    lastPk: Option[String] = None
  ): Unit = {
    // Recreate connection factory per operation (CDM pattern)
    val connectionFactory = new YugabyteConnectionFactory(yugabyteConfig)
    var conn: Option[Connection] = None
    try {
      conn = Some(connectionFactory.getConnection())
      
      val sql =
        s"""
           |UPDATE $checkpointTable
           |SET status = ?, rows_written = ?, last_pk = ?, updated_at = now()
           |WHERE job_id = ? AND partition_id = ?
           |""".stripMargin
      
      val stmt = conn.get.prepareStatement(sql)
      stmt.setString(1, status)
      stmt.setLong(2, rowsWritten)
      stmt.setString(3, lastPk.orNull)
      stmt.setString(4, jobId)
      stmt.setInt(5, partitionId)
      stmt.executeUpdate()
      
    } catch {
      case e: Exception =>
        logError(s"Error updating checkpoint: ${e.getMessage}", e)
        // Don't throw - checkpoint updates shouldn't fail the migration
    } finally {
      conn.foreach(_.close())
    }
  }
  
  /**
   * Get checkpoint status for a partition
   */
  def getCheckpointStatus(jobId: String, partitionId: Int): Option[CheckpointStatus] = {
    // Recreate connection factory per operation (CDM pattern)
    val connectionFactory = new YugabyteConnectionFactory(yugabyteConfig)
    var conn: Option[Connection] = None
    try {
      conn = Some(connectionFactory.getConnection())
      
      val sql =
        s"""
           |SELECT status, rows_written, last_pk, token_start, token_end
           |FROM $checkpointTable
           |WHERE job_id = ? AND partition_id = ?
           |""".stripMargin
      
      val stmt = conn.get.prepareStatement(sql)
      stmt.setString(1, jobId)
      stmt.setInt(2, partitionId)
      val rs = stmt.executeQuery()
      
      if (rs.next()) {
        Some(CheckpointStatus(
          status = rs.getString("status"),
          rowsWritten = rs.getLong("rows_written"),
          lastPk = Option(rs.getString("last_pk")),
          tokenStart = rs.getLong("token_start"),
          tokenEnd = rs.getLong("token_end")
        ))
      } else {
        None
      }
    } catch {
      case e: Exception =>
        logError(s"Error getting checkpoint status: ${e.getMessage}", e)
        None
    } finally {
      conn.foreach(_.close())
    }
  }
  
  /**
   * Get all pending partitions for a job
   */
  def getPendingPartitions(jobId: String): List[Int] = {
    // Recreate connection factory per operation (CDM pattern)
    val connectionFactory = new YugabyteConnectionFactory(yugabyteConfig)
    var conn: Option[Connection] = None
    try {
      conn = Some(connectionFactory.getConnection())
      
      val sql =
        s"""
           |SELECT partition_id
           |FROM $checkpointTable
           |WHERE job_id = ? AND status IN ('PENDING', 'RUNNING', 'FAILED')
           |""".stripMargin
      
      val stmt = conn.get.prepareStatement(sql)
      stmt.setString(1, jobId)
      val rs = stmt.executeQuery()
      
      val partitions = scala.collection.mutable.ListBuffer[Int]()
      while (rs.next()) {
        partitions += rs.getInt("partition_id")
      }
      partitions.toList
    } catch {
      case e: Exception =>
        logError(s"Error getting pending partitions: ${e.getMessage}", e)
        List.empty
    } finally {
      conn.foreach(_.close())
    }
  }
}

case class CheckpointStatus(
  status: String,
  rowsWritten: Long,
  lastPk: Option[String],
  tokenStart: Long,
  tokenEnd: Long
)


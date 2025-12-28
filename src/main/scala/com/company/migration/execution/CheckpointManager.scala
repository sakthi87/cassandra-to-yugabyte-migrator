package com.company.migration.execution

import com.company.migration.config.YugabyteConfig
import com.company.migration.util.Logging
import com.company.migration.yugabyte.YugabyteConnectionFactory
import java.sql.{Connection, PreparedStatement, ResultSet, Timestamp}
import java.time.Instant
import scala.collection.mutable.ListBuffer

/**
 * Manages checkpointing for migration jobs
 * Uses a two-table design for robust checkpointing:
 * - migration_run_info: Run-level metadata
 * - migration_run_details: Partition/token-range level tracking
 * 
 * Pattern: Creates connections per operation (not stored)
 * This ensures serializability and follows best practices for Spark
 */
// Status constants for run tracking
object CheckpointManager {
  object RunStatus {
    val NOT_STARTED = "NOT_STARTED"
    val STARTED = "STARTED"
    val PASS = "PASS"
    val FAIL = "FAIL"
    val ENDED = "ENDED"
  }
}

class CheckpointManager(
  val yugabyteConfig: YugabyteConfig,
  val keyspaceName: String = "public"
) extends Logging with Serializable {
  
  val runInfoTable = s"$keyspaceName.migration_run_info"
  val runDetailsTable = s"$keyspaceName.migration_run_details"
  
  /**
   * Initialize checkpoint tables if they don't exist
   * Creates both run_info and run_details tables
   */
  def initializeCheckpointTables(): Unit = {
    val connectionFactory = new YugabyteConnectionFactory(yugabyteConfig)
    var conn: Option[Connection] = None
    try {
      conn = Some(connectionFactory.getConnection())
      val stmt = conn.get.createStatement()
      
      // Create run_info table (run-level metadata)
      val createRunInfoSql =
        s"""
           |CREATE TABLE IF NOT EXISTS $runInfoTable (
           |    table_name      TEXT,
           |    run_id          BIGINT,
           |    run_type        TEXT,
           |    prev_run_id     BIGINT,
           |    start_time      TIMESTAMPTZ DEFAULT now(),
           |    end_time        TIMESTAMPTZ,
           |    run_info        TEXT,
           |    status          TEXT,
           |    PRIMARY KEY (table_name, run_id)
           |);
           |""".stripMargin
      
      stmt.execute(createRunInfoSql)
      logInfo(s"Checkpoint table '$runInfoTable' initialized")
      
      // Create run_details table (partition/token-range level tracking)
      val createRunDetailsSql =
        s"""
           |CREATE TABLE IF NOT EXISTS $runDetailsTable (
           |    table_name      TEXT,
           |    run_id          BIGINT,
           |    start_time      TIMESTAMPTZ DEFAULT now(),
           |    token_min       BIGINT,
           |    token_max       BIGINT,
           |    partition_id    INT,
           |    status          TEXT,
           |    run_info        TEXT,
           |    PRIMARY KEY ((table_name, run_id), token_min, partition_id)
           |);
           |""".stripMargin
      
      stmt.execute(createRunDetailsSql)
      logInfo(s"Checkpoint table '$runDetailsTable' initialized")
      
      // Create indexes for efficient querying
      try {
        stmt.execute(s"CREATE INDEX IF NOT EXISTS idx_run_details_status ON $runDetailsTable (table_name, run_id, status)")
        stmt.execute(s"CREATE INDEX IF NOT EXISTS idx_run_info_status ON $runInfoTable (table_name, status)")
      } catch {
        case e: Exception =>
          logWarn(s"Could not create indexes (may already exist): ${e.getMessage}")
      }
      
    } catch {
      case e: Exception =>
        logError(s"Error initializing checkpoint tables: ${e.getMessage}", e)
        throw e
    } finally {
      conn.foreach(_.close())
    }
  }
  
  /**
   * Get pending partitions from a previous run
   * Returns partitions with status: NOT_STARTED, STARTED, or FAIL
   * Only retries incomplete/failed partitions
   */
  def getPendingPartitions(
    tableName: String,
    prevRunId: Long,
    runType: String = "MIGRATE"
  ): List[PartitionCheckpoint] = {
    if (prevRunId == 0) {
      return List.empty
    }
    
    val connectionFactory = new YugabyteConnectionFactory(yugabyteConfig)
    var conn: Option[Connection] = None
    try {
      conn = Some(connectionFactory.getConnection())
      
      // First, verify the previous run exists and was started
      val checkRunSql =
        s"""
           |SELECT status FROM $runInfoTable
           |WHERE table_name = ? AND run_id = ?
           |""".stripMargin
      
      val checkStmt = conn.get.prepareStatement(checkRunSql)
      checkStmt.setString(1, tableName)
      checkStmt.setLong(2, prevRunId)
      val checkRs = checkStmt.executeQuery()
      
      if (!checkRs.next()) {
        logWarn(s"Previous run $prevRunId not found for table $tableName. Starting new run.")
        return List.empty
      }
      
      val prevRunStatus = checkRs.getString("status")
      if (prevRunStatus == CheckpointManager.RunStatus.NOT_STARTED) {
        logWarn(s"Previous run $prevRunId was not started. Starting new run.")
        return List.empty
      }
      
      // Get pending partitions (NOT_STARTED, STARTED, FAIL)
      val pendingStatuses = Array(CheckpointManager.RunStatus.NOT_STARTED, CheckpointManager.RunStatus.STARTED, CheckpointManager.RunStatus.FAIL)
      val pendingParts = ListBuffer[PartitionCheckpoint]()
      
      for (status <- pendingStatuses) {
        val sql =
          s"""
             |SELECT token_min, token_max, partition_id, run_info
             |FROM $runDetailsTable
             |WHERE table_name = ? AND run_id = ? AND status = ?
             |""".stripMargin
        
        val stmt = conn.get.prepareStatement(sql)
        stmt.setString(1, tableName)
        stmt.setLong(2, prevRunId)
        stmt.setString(3, status)
        val rs = stmt.executeQuery()
        
        while (rs.next()) {
          pendingParts += PartitionCheckpoint(
            tokenMin = rs.getLong("token_min"),
            tokenMax = rs.getLong("token_max"),
            partitionId = rs.getInt("partition_id"),
            runInfo = Option(rs.getString("run_info"))
          )
        }
      }
      
      logInfo(s"Found ${pendingParts.size} pending partitions from previous run $prevRunId for table $tableName")
      pendingParts.toList
      
    } catch {
      case e: Exception =>
        logError(s"Error getting pending partitions: ${e.getMessage}", e)
        List.empty
    } finally {
      conn.foreach(_.close())
    }
  }
  
  /**
   * Initialize a new migration run
   * Creates run_info entry and run_details entries for all partitions
   */
  def initRun(
    tableName: String,
    runId: Long,
    prevRunId: Long,
    partitions: List[PartitionCheckpoint],
    runType: String = "MIGRATE"
  ): Unit = {
    val connectionFactory = new YugabyteConnectionFactory(yugabyteConfig)
    var conn: Option[Connection] = None
    try {
      conn = Some(connectionFactory.getConnection())
      
      // Check if run_id already exists
      val checkSql = s"SELECT run_id FROM $runInfoTable WHERE table_name = ? AND run_id = ?"
      val checkStmt = conn.get.prepareStatement(checkSql)
      checkStmt.setString(1, tableName)
      checkStmt.setLong(2, runId)
      val checkRs = checkStmt.executeQuery()
      
      if (checkRs.next()) {
        throw new RuntimeException(s"Run id $runId already exists for table $tableName")
      }
      
      // Insert run_info entry with NOT_STARTED status
      val runInfoSql =
        s"""
           |INSERT INTO $runInfoTable
           |(table_name, run_id, run_type, prev_run_id, start_time, status)
           |VALUES (?, ?, ?, ?, now(), ?)
           |""".stripMargin
      
      val runInfoStmt = conn.get.prepareStatement(runInfoSql)
      runInfoStmt.setString(1, tableName)
      runInfoStmt.setLong(2, runId)
      runInfoStmt.setString(3, runType)
      runInfoStmt.setLong(4, prevRunId)
      runInfoStmt.setString(5, CheckpointManager.RunStatus.NOT_STARTED)
      runInfoStmt.executeUpdate()
      
      // Insert run_details entries for each partition
      val detailsSql =
        s"""
           |INSERT INTO $runDetailsTable
           |(table_name, run_id, token_min, token_max, partition_id, status)
           |VALUES (?, ?, ?, ?, ?, ?)
           |""".stripMargin
      
      val detailsStmt = conn.get.prepareStatement(detailsSql)
      partitions.foreach { part =>
        detailsStmt.setString(1, tableName)
        detailsStmt.setLong(2, runId)
        detailsStmt.setLong(3, part.tokenMin)
        detailsStmt.setLong(4, part.tokenMax)
        detailsStmt.setInt(5, part.partitionId)
        detailsStmt.setString(6, CheckpointManager.RunStatus.NOT_STARTED)
        detailsStmt.addBatch()
      }
      detailsStmt.executeBatch()
      
      // Update run_info status to STARTED
      val updateRunInfoSql =
        s"""
           |UPDATE $runInfoTable
           |SET status = ?
           |WHERE table_name = ? AND run_id = ?
           |""".stripMargin
      
      val updateStmt = conn.get.prepareStatement(updateRunInfoSql)
      updateStmt.setString(1, CheckpointManager.RunStatus.STARTED)
      updateStmt.setString(2, tableName)
      updateStmt.setLong(3, runId)
      updateStmt.executeUpdate()
      
      conn.get.commit()
      logInfo(s"Initialized run $runId for table $tableName with ${partitions.size} partitions")
      
    } catch {
      case e: Exception =>
        conn.foreach(_.rollback())
        logError(s"Error initializing run: ${e.getMessage}", e)
        throw e
    } finally {
      conn.foreach(_.close())
    }
  }
  
  /**
   * Update checkpoint for a partition
   * Updates run_details with status and metrics
   */
  def updateRun(
    tableName: String,
    runId: Long,
    tokenMin: Long,
    partitionId: Int,
    status: String,
    runInfo: Option[String] = None
  ): Unit = {
    val connectionFactory = new YugabyteConnectionFactory(yugabyteConfig)
    var conn: Option[Connection] = None
    try {
      conn = Some(connectionFactory.getConnection())
      
      if (status == CheckpointManager.RunStatus.STARTED) {
        // Update start_time when status changes to STARTED
        val sql =
          s"""
             |UPDATE $runDetailsTable
             |SET start_time = now(), status = ?
             |WHERE table_name = ? AND run_id = ? AND token_min = ? AND partition_id = ?
             |""".stripMargin
        
        val stmt = conn.get.prepareStatement(sql)
        stmt.setString(1, status)
        stmt.setString(2, tableName)
        stmt.setLong(3, runId)
        stmt.setLong(4, tokenMin)
        stmt.setInt(5, partitionId)
        stmt.executeUpdate()
      } else {
        // Update status and run_info for other statuses
        val sql =
          s"""
             |UPDATE $runDetailsTable
             |SET status = ?, run_info = ?
             |WHERE table_name = ? AND run_id = ? AND token_min = ? AND partition_id = ?
             |""".stripMargin
        
        val stmt = conn.get.prepareStatement(sql)
        stmt.setString(1, status)
        stmt.setString(2, runInfo.orNull)
        stmt.setString(3, tableName)
        stmt.setLong(4, runId)
        stmt.setLong(5, tokenMin)
        stmt.setInt(6, partitionId)
        stmt.executeUpdate()
      }
      
      conn.get.commit()
      
    } catch {
      case e: Exception =>
        logError(s"Error updating checkpoint: ${e.getMessage}", e)
        // Don't throw - checkpoint updates shouldn't fail the migration
    } finally {
      conn.foreach(_.close())
    }
  }
  
  /**
   * End a migration run
   * Updates run_info with end_time, final status, and metrics
   */
  def endRun(
    tableName: String,
    runId: Long,
    runInfo: String
  ): Unit = {
    val connectionFactory = new YugabyteConnectionFactory(yugabyteConfig)
    var conn: Option[Connection] = None
    try {
      conn = Some(connectionFactory.getConnection())
      
      val sql =
        s"""
           |UPDATE $runInfoTable
           |SET end_time = now(), run_info = ?, status = ?
           |WHERE table_name = ? AND run_id = ?
           |""".stripMargin
      
      val stmt = conn.get.prepareStatement(sql)
      stmt.setString(1, runInfo)
      stmt.setString(2, CheckpointManager.RunStatus.ENDED)
      stmt.setString(3, tableName)
      stmt.setLong(4, runId)
      stmt.executeUpdate()
      
      conn.get.commit()
      logInfo(s"Ended run $runId for table $tableName")
      
    } catch {
      case e: Exception =>
        logError(s"Error ending run: ${e.getMessage}", e)
        // Don't throw - ending run shouldn't fail
    } finally {
      conn.foreach(_.close())
    }
  }
}

/**
 * Represents a partition checkpoint
 */
case class PartitionCheckpoint(
  tokenMin: Long,
  tokenMax: Long,
  partitionId: Int,
  runInfo: Option[String] = None
)

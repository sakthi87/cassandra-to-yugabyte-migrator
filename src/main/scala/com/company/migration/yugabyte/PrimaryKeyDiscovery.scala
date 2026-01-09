package com.company.migration.yugabyte

import com.company.migration.config.TableConfig
import com.company.migration.util.Logging

import java.sql.Connection

/**
 * Utility to discover primary key columns from YugabyteDB table
 */
object PrimaryKeyDiscovery extends Logging {
  
  /**
   * Get primary key columns from table
   * First tries tableConfig.primaryKey, then discovers from database if empty
   */
  def getPrimaryKeyColumns(
    connection: Connection,
    tableConfig: TableConfig,
    allColumns: List[String]
  ): List[String] = {
    // Use configured primary key if available
    if (tableConfig.primaryKey.nonEmpty) {
      logDebug(s"Using configured primary key columns: ${tableConfig.primaryKey.mkString(", ")}")
      return tableConfig.primaryKey
    }
    
    // Discover from database
    try {
      val metadata = connection.getMetaData
      val schema = tableConfig.targetSchema
      val tableName = tableConfig.targetTable
      
      val primaryKeys = scala.collection.mutable.ListBuffer[String]()
      val rs = metadata.getPrimaryKeys(null, schema, tableName)
      
      while (rs.next()) {
        val columnName = rs.getString("COLUMN_NAME")
        val keySeq = rs.getShort("KEY_SEQ")
        primaryKeys += columnName
        logDebug(s"Discovered primary key column: $columnName (sequence: $keySeq)")
      }
      rs.close()
      
      if (primaryKeys.isEmpty) {
        logWarn(s"Could not discover primary key columns for ${schema}.${tableName}. Using first column as fallback.")
        // Fallback: use first column
        List(allColumns.head)
      } else {
        logInfo(s"Discovered ${primaryKeys.size} primary key columns: ${primaryKeys.mkString(", ")}")
        primaryKeys.toList
      }
    } catch {
      case e: Exception =>
        logWarn(s"Error discovering primary key columns: ${e.getMessage}. Using first column as fallback.")
        List(allColumns.head)
    }
  }
}


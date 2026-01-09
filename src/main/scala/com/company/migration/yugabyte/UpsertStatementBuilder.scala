package com.company.migration.yugabyte

import com.company.migration.config.TableConfig

/**
 * Builds INSERT ... ON CONFLICT DO NOTHING SQL statements for YugabyteDB
 * Used for idempotent inserts that handle duplicates gracefully
 */
object UpsertStatementBuilder {
  
  /**
   * Build INSERT ... ON CONFLICT DO NOTHING SQL statement
   * Format: INSERT INTO schema.table (col1, col2, ...) VALUES (?, ?, ...) ON CONFLICT (pk1, pk2, ...) DO NOTHING
   * 
   * @param tableConfig Table configuration
   * @param columns All column names (including constant columns)
   * @param primaryKeyColumns Primary key column names (for ON CONFLICT clause)
   */
  def buildUpsertStatement(
    tableConfig: TableConfig,
    columns: List[String],
    primaryKeyColumns: List[String]
  ): String = {
    val columnList = columns.mkString(", ")
    val schemaTable = s"${tableConfig.targetSchema}.${tableConfig.targetTable}"
    val placeholders = columns.map(_ => "?").mkString(", ")
    val primaryKeyList = primaryKeyColumns.mkString(", ")
    
    s"""INSERT INTO $schemaTable ($columnList)
       |VALUES ($placeholders)
       |ON CONFLICT ($primaryKeyList) DO NOTHING""".stripMargin
  }
}


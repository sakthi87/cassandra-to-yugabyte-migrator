package com.company.migration.yugabyte

import com.company.migration.config.TableConfig
import com.company.migration.config.YugabyteConfig

/**
 * Builds COPY FROM STDIN SQL statements for YugabyteDB
 */
object CopyStatementBuilder {
  
  /**
   * Build COPY SQL statement
   * Format: COPY schema.table (col1, col2, ...) FROM STDIN WITH (FORMAT csv, REPLACE, ...)
   * 
   * If copyReplace is true, adds REPLACE option for idempotent upsert behavior:
   * - Replaces existing rows with same primary key
   * - Inserts new rows if primary key doesn't exist
   * - Makes COPY operations idempotent and safe for retries
   */
  def buildCopyStatement(
    tableConfig: TableConfig,
    columns: List[String],
    yugabyteConfig: YugabyteConfig
  ): String = {
    val columnList = columns.mkString(", ")
    val schemaTable = s"${tableConfig.targetSchema}.${tableConfig.targetTable}"
    
    // Build WITH clause options
    val withOptions = scala.collection.mutable.ListBuffer[String]()
    withOptions += "FORMAT csv"
    withOptions += s"DELIMITER '${yugabyteConfig.csvDelimiter}'"
    withOptions += s"NULL '${yugabyteConfig.csvNull}'"
    withOptions += s"QUOTE '${yugabyteConfig.csvQuote}'"
    withOptions += s"ESCAPE '${yugabyteConfig.csvEscape}'"
    
    // Add REPLACE option if enabled (for idempotent upsert behavior)
    if (yugabyteConfig.copyReplace) {
      withOptions += "REPLACE"
    }
    
    s"""COPY $schemaTable ($columnList)
       |FROM STDIN
       |WITH (
       |  ${withOptions.mkString(",\n  ")}
       |)""".stripMargin
  }
}


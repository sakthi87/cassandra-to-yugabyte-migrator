package com.company.migration.yugabyte

import com.company.migration.config.TableConfig
import com.company.migration.config.YugabyteConfig

/**
 * Builds COPY FROM STDIN SQL statements for YugabyteDB
 */
object CopyStatementBuilder {
  
  /**
   * Build COPY SQL statement
   * Format: COPY schema.table (col1, col2, ...) FROM STDIN WITH (FORMAT csv, ...)
   */
  def buildCopyStatement(
    tableConfig: TableConfig,
    columns: List[String],
    yugabyteConfig: YugabyteConfig
  ): String = {
    val columnList = columns.mkString(", ")
    val schemaTable = s"${tableConfig.targetSchema}.${tableConfig.targetTable}"
    
    s"""COPY $schemaTable ($columnList)
       |FROM STDIN
       |WITH (
       |  FORMAT csv,
       |  DELIMITER '${yugabyteConfig.csvDelimiter}',
       |  NULL '${yugabyteConfig.csvNull}',
       |  QUOTE '${yugabyteConfig.csvQuote}',
       |  ESCAPE '${yugabyteConfig.csvEscape}'
       |)""".stripMargin
  }
}


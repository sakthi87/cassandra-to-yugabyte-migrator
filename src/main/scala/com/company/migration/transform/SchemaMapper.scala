package com.company.migration.transform

import com.company.migration.config.TableConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
 * Maps Cassandra schema to YugabyteDB schema
 * Handles column name mapping and type compatibility
 */
object SchemaMapper {
  
  /**
   * Get target column names in order
   * Applies column mapping if specified
   */
  def getTargetColumns(sourceSchema: StructType, tableConfig: TableConfig): List[String] = {
    if (tableConfig.columnMapping.isEmpty) {
      // No mapping - use source column names
      sourceSchema.fieldNames.toList
    } else {
      // Apply column mapping
      sourceSchema.fieldNames.map { sourceCol =>
        tableConfig.columnMapping.getOrElse(sourceCol, sourceCol)
      }.toList
    }
  }
  
  /**
   * Get source column name for a target column
   */
  def getSourceColumnName(targetColumn: String, tableConfig: TableConfig): String = {
    // Reverse lookup in column mapping
    tableConfig.columnMapping.find(_._2 == targetColumn)
      .map(_._1)
      .getOrElse(targetColumn)
  }
  
  /**
   * Validate that all source columns exist in the DataFrame
   */
  def validateSourceColumns(df: DataFrame, tableConfig: TableConfig): Unit = {
    val sourceColumns = df.schema.fieldNames.toSet
    
    // Check if mapped columns exist
    tableConfig.columnMapping.keys.foreach { sourceCol =>
      if (!sourceColumns.contains(sourceCol)) {
        throw new IllegalArgumentException(
          s"Source column '$sourceCol' not found in DataFrame. Available columns: ${sourceColumns.mkString(", ")}"
        )
      }
    }
  }
}


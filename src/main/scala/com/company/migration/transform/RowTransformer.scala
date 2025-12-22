package com.company.migration.transform

import com.company.migration.config.TableConfig
import com.company.migration.util.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

/**
 * Transforms Spark Rows to CSV format for COPY FROM STDIN
 * Handles escaping, quoting, and null handling
 */
class RowTransformer(tableConfig: TableConfig, targetColumns: List[String], sourceSchema: StructType) extends Logging {
  
  /**
   * Convert a Row to CSV string
   * Returns null if row should be skipped (e.g., null primary key)
   */
  def toCsv(row: Row): Option[String] = {
    try {
      val values = targetColumns.map { targetCol =>
        val sourceCol = SchemaMapper.getSourceColumnName(targetCol, tableConfig)
        val fieldIndex = sourceSchema.fieldIndex(sourceCol)
        val dataType = sourceSchema.fields(fieldIndex).dataType
        val value = row.get(fieldIndex)
        
        val stringValue = DataTypeConverter.convertToString(value, dataType)
        escapeCsvField(stringValue)
      }
      
      Some(values.mkString(","))
    } catch {
      case e: Exception =>
        logWarn(s"Error transforming row to CSV: ${e.getMessage}")
        None
    }
  }
  
  /**
   * Escape a CSV field according to PostgreSQL CSV format rules
   * - Fields containing delimiter, quote, or newline must be quoted
   * - Fields with leading/trailing whitespace should be quoted
   * - Quotes within quoted fields are escaped by doubling
   */
  private def escapeCsvField(field: String): String = {
    if (field.isEmpty) {
      field // Empty string (represents NULL)
    } else {
      val needsQuoting = field.contains(",") || 
                        field.contains("\"") || 
                        field.contains("\n") || 
                        field.contains("\r") ||
                        field.startsWith(" ") ||
                        field.endsWith(" ") ||
                        field.startsWith("\t") ||
                        field.endsWith("\t")
      
      if (needsQuoting) {
        // Escape quotes by doubling them
        val escaped = field.replace("\"", "\"\"")
        s""""$escaped""""
      } else {
        field
      }
    }
  }
  
  /**
   * Remove null bytes (0x00) which are invalid in UTF-8
   */
  private def removeNullBytes(str: String): String = {
    str.replace("\u0000", "")
  }
}


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
        
        // CRITICAL: Check if value is null using Spark's isNullAt (not Java null check)
        // This correctly handles null values vs empty strings vs whitespace-only strings
        val isNull = row.isNullAt(fieldIndex)
        
        // If null, skip convertToString and pass empty string directly
        // If not null, convert to string first
        val stringValue = if (isNull) {
          "" // Empty string for NULL (will be handled by escapeCsvField)
        } else {
          val value = row.get(fieldIndex)
          DataTypeConverter.convertToString(value, dataType)
        }
        
        escapeCsvField(stringValue, isNull)
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
   * - NULL values: Empty string (as per yugabyte.csvNull=)
   * - Empty strings: Must be quoted to distinguish from NULL
   * - Whitespace-only strings: Must be quoted to preserve whitespace
   * - Fields containing delimiter, quote, or newline must be quoted
   * - Fields with leading/trailing whitespace should be quoted
   * - Quotes within quoted fields are escaped by doubling
   * - Non-ASCII characters: Must be properly UTF-8 encoded and quoted if needed
   */
  private def escapeCsvField(field: String, isNull: Boolean): String = {
    // CRITICAL: NULL values become empty string (PostgreSQL COPY NULL representation)
    if (isNull) {
      return "" // Empty string represents NULL in CSV
    }
    
    // CRITICAL: Empty strings must be quoted to distinguish from NULL
    // PostgreSQL COPY treats unquoted empty string as NULL
    if (field.isEmpty) {
      return "\"\"" // Quoted empty string represents actual empty string (not NULL)
    }
    
    // CRITICAL: Whitespace-only strings must be quoted to preserve whitespace
    // Unquoted whitespace-only strings may be trimmed by PostgreSQL COPY
    val isWhitespaceOnly = field.trim.isEmpty && field.nonEmpty
    
    val needsQuoting = isWhitespaceOnly || // Whitespace-only strings
                        field.contains(",") || // Contains delimiter
                        field.contains("\"") || // Contains quote
                        field.contains("\n") || // Contains newline
                        field.contains("\r") || // Contains carriage return
                        field.startsWith(" ") || // Leading space
                        field.endsWith(" ") || // Trailing space
                        field.startsWith("\t") || // Leading tab
                        field.endsWith("\t") || // Trailing tab
                        !field.matches("^[\\x20-\\x7E]*$") // Contains non-ASCII characters
    
    if (needsQuoting) {
      // Remove null bytes (0x00) which are invalid in UTF-8
      val cleaned = removeNullBytes(field)
      // Escape quotes by doubling them
      val escaped = cleaned.replace("\"", "\"\"")
      s""""$escaped""""
    } else {
      // Remove null bytes even from unquoted fields
      removeNullBytes(field)
    }
  }
  
  /**
   * Remove null bytes (0x00) which are invalid in UTF-8
   */
  private def removeNullBytes(str: String): String = {
    str.replace("\u0000", "")
  }
}


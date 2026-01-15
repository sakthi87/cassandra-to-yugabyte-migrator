package com.company.migration.transform

import com.company.migration.config.TableConfig
import com.company.migration.util.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Transforms Spark Rows to CSV format for COPY FROM STDIN
 * Handles escaping, quoting, and null handling
 */
class RowTransformer(tableConfig: TableConfig, targetColumns: List[String], sourceSchema: StructType) extends Logging {
  
  /**
   * Convert a Row to CSV string
   * Returns null if row should be skipped (e.g., null primary key)
   * Includes constant columns (default values) at the end
   */
  def toCsv(row: Row): Option[String] = {
    try {
      // Split target columns into source columns and constant columns
      val (sourceTargetCols, constantCols) = targetColumns.partition { targetCol =>
        // Check if this is a constant column (has a default value)
        !tableConfig.constantColumns.contains(targetCol)
      }
      
      // Transform source columns
      val sourceValues = sourceTargetCols.map { targetCol =>
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
      
      // Add constant column values (parse configured value and render for CSV)
      val constantValues = constantCols.map { constantCol =>
        val constantValue = tableConfig.constantColumns(constantCol)
        val resolvedValue = resolveConstantValueForCsv(constantValue)
        escapeCsvField(resolvedValue, isNull = false)
      }
      
      Some((sourceValues ++ constantValues).mkString(","))
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
  
  /**
   * Convert a Row to Array[Any] for INSERT statements with PreparedStatement
   * Returns None if row should be skipped (e.g., null primary key)
   * Includes constant columns (default values) at the end
   */
  def toValues(row: Row): Option[Array[Any]] = {
    try {
      // Split target columns into source columns and constant columns
      val (sourceTargetCols, constantCols) = targetColumns.partition { targetCol =>
        !tableConfig.constantColumns.contains(targetCol)
      }
      
      // Transform source columns to values
      val sourceValues = sourceTargetCols.map { targetCol =>
        val sourceCol = SchemaMapper.getSourceColumnName(targetCol, tableConfig)
        val fieldIndex = sourceSchema.fieldIndex(sourceCol)
        val dataType = sourceSchema.fields(fieldIndex).dataType
        
        // Check if value is null
        if (row.isNullAt(fieldIndex)) {
          null
        } else {
          // Get value and convert to appropriate type for JDBC
          val value = row.get(fieldIndex)
          convertToJdbcType(value, dataType)
        }
      }
      
      // Add constant column values (parse from string configuration)
      val constantValues = constantCols.map { constantCol =>
        val constantValueStr = tableConfig.constantColumns(constantCol)
        // Parse constant value (basic parsing - could be enhanced)
        parseConstantValue(constantValueStr)
      }
      
      Some((sourceValues ++ constantValues).toArray)
    } catch {
      case e: Exception =>
        logWarn(s"Error transforming row to values: ${e.getMessage}")
        None
    }
  }
  
  /**
   * Convert value to JDBC-compatible type
   */
  private def convertToJdbcType(value: Any, dataType: DataType): Any = {
    // For most types, Spark Row already provides the correct Java types
    // This is a simplified conversion - can be enhanced for edge cases
    value match {
      case timestamp: java.sql.Timestamp => timestamp
      case date: java.sql.Date => date
      case instant: java.time.Instant => java.sql.Timestamp.from(instant)
      case ldt: java.time.LocalDateTime => java.sql.Timestamp.valueOf(ldt)
      case _ => value  // Use as-is for primitives (String, Int, Long, Double, Boolean, etc.)
    }
  }
  
  /**
   * Parse constant value from string configuration
   * Supports basic types: strings (with quotes), numbers, booleans
   */
  private def parseConstantValue(valueStr: String): Any = {
    val trimmed = valueStr.trim
    if (isCurrentTimestampValue(trimmed)) {
      return currentTimestamp()
    }
    // Remove surrounding quotes if present
    if (trimmed.startsWith("'") && trimmed.endsWith("'") && trimmed.length > 1) {
      val unquoted = trimmed.substring(1, trimmed.length - 1)
      if (isCurrentTimestampValue(unquoted)) {
        currentTimestamp()
      } else {
        unquoted
      }
    } else if (trimmed.startsWith("\"") && trimmed.endsWith("\"") && trimmed.length > 1) {
      val unquoted = trimmed.substring(1, trimmed.length - 1)
      if (isCurrentTimestampValue(unquoted)) {
        currentTimestamp()
      } else {
        unquoted
      }
    } else if (trimmed.toLowerCase == "true") {
      true
    } else if (trimmed.toLowerCase == "false") {
      false
    } else if (trimmed.matches("^-?\\d+$")) {
      // Integer
      try {
        trimmed.toLong  // Use Long to handle large integers
      } catch {
        case _: NumberFormatException => trimmed
      }
    } else if (trimmed.matches("^-?\\d+\\.\\d+$")) {
      // Double
      try {
        trimmed.toDouble
      } catch {
        case _: NumberFormatException => trimmed
      }
    } else {
      // String (default)
      trimmed
    }
  }

  private def resolveConstantValueForCsv(valueStr: String): String = {
    parseConstantValue(valueStr) match {
      case ts: java.sql.Timestamp => ts.toString
      case date: java.sql.Date => date.toString
      case other => other.toString
    }
  }

  private def isCurrentTimestampValue(valueStr: String): Boolean = {
    val normalized = valueStr.trim.toUpperCase
    normalized == "CURRENTTIMESTAMP" || normalized == "CURRENT_TIMESTAMP"
  }

  private def currentTimestamp(): java.sql.Timestamp = {
    java.sql.Timestamp.from(java.time.Instant.now())
  }
}


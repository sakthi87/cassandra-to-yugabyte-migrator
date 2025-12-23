package com.company.migration.transform

import java.sql.Timestamp
import java.time.{Instant, LocalDateTime, ZoneOffset}
import java.util.UUID
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * Converts Cassandra data types to YugabyteDB-compatible formats
 */
object DataTypeConverter {
  private val objectMapper = new ObjectMapper()
  objectMapper.registerModule(DefaultScalaModule)
  
  /**
   * Convert a value to its string representation for CSV
   * Handles nulls, UUIDs, timestamps, collections, etc.
   * 
   * CRITICAL: This method should NOT be called with null values.
   * Use row.isNullAt() to check for nulls before calling this method.
   * This ensures proper handling of:
   * - NULL values (empty string in CSV)
   * - Empty strings ("")
   * - Whitespace-only strings ("   ")
   * - Non-ASCII characters
   */
  def convertToString(value: Any, dataType: DataType): String = {
    // This should only be called with non-null values
    // Null checking is done in RowTransformer using row.isNullAt()
    if (value == null) {
      throw new IllegalArgumentException("convertToString should not be called with null. Use row.isNullAt() to check for nulls first.")
    }
    
    dataType match {
      case StringType => 
        // Preserve the exact string value, including spaces and non-ASCII characters
        value.toString
      case IntegerType => value.toString
      case LongType => value.toString
      case DoubleType => value.toString
      case FloatType => value.toString
      case BooleanType => value.toString
      case TimestampType => convertTimestamp(value)
      case DateType => value.toString
      case BinaryType => convertBinary(value)
      case _ => convertComplexType(value, dataType)
    }
  }
  
  private def convertTimestamp(value: Any): String = {
    value match {
      case ts: Timestamp => ts.toInstant.toString
      case instant: Instant => instant.toString
      case ldt: LocalDateTime => ldt.toInstant(ZoneOffset.UTC).toString
      case long: Long => Instant.ofEpochMilli(long).toString
      case _ => value.toString
    }
  }
  
  private def convertBinary(value: Any): String = {
    value match {
      case bytes: Array[Byte] => java.util.Base64.getEncoder.encodeToString(bytes)
      case _ => value.toString
    }
  }
  
  private def convertComplexType(value: Any, dataType: DataType): String = {
    // For collections (List, Map, Set), convert to JSON
    try {
      objectMapper.writeValueAsString(value)
    } catch {
      case _: Exception => value.toString
    }
  }
  
  /**
   * Get value from Row by column name (case-insensitive)
   */
  def getRowValue(row: Row, columnName: String): Any = {
    try {
      row.getAs(columnName)
    } catch {
      case _: IllegalArgumentException =>
        // Try case-insensitive match
        val fieldIndex = row.schema.fieldIndex(columnName.toLowerCase)
        row.get(fieldIndex)
      case e: Exception =>
        throw new RuntimeException(s"Error getting value for column '$columnName': ${e.getMessage}", e)
    }
  }
}


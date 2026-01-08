package com.company.migration.config

import java.util.Properties
import scala.jdk.CollectionConverters._

/**
 * Configuration for a single table migration
 */
case class TableConfig(
  sourceKeyspace: String,
  sourceTable: String,
  targetSchema: String,
  targetTable: String,
  columnMapping: Map[String, String],
  typeMapping: Map[String, String],
  primaryKey: List[String],
  validate: Boolean,
  constantColumns: Map[String, String] = Map.empty  // Column name -> constant value (for audit fields)
)

object TableConfig {
  def fromProperties(props: Properties): TableConfig = {
    def getProperty(key: String, default: String = ""): String = {
      props.getProperty(key, default)
    }
    
    def getBooleanProperty(key: String, default: Boolean): Boolean = {
      val value = props.getProperty(key)
      if (value != null && value.nonEmpty) value.toBoolean else default
    }
    
    // Extract column mapping
    val columnMapping = props.stringPropertyNames().asScala
      .filter(_.startsWith("table.columnMapping."))
      .map { key =>
        val cassandraCol = key.replace("table.columnMapping.", "")
        val yugabyteCol = props.getProperty(key)
        cassandraCol -> yugabyteCol
      }
      .toMap
    
    // Extract type mapping
    val typeMapping = props.stringPropertyNames().asScala
      .filter(_.startsWith("table.typeMapping."))
      .map { key =>
        val cassandraType = key.replace("table.typeMapping.", "")
        val yugabyteType = props.getProperty(key)
        cassandraType -> yugabyteType
      }
      .toMap
    
    // Extract primary key
    val primaryKeyStr = getProperty("table.primaryKey", "")
    val primaryKey = if (primaryKeyStr.nonEmpty) {
      primaryKeyStr.split(",").map(_.trim).filter(_.nonEmpty).toList
    } else {
      List.empty[String]
    }
    
    // Extract constant columns (default values for target columns not in source)
    // Format: table.constantColumns.names=col1,col2,col3
    //         table.constantColumns.values=val1,val2,val3
    //         table.constantColumns.splitRegex=, (optional, defaults to comma)
    val constantColumns = {
      val namesStr = getProperty("table.constantColumns.names", "")
      val valuesStr = getProperty("table.constantColumns.values", "")
      val splitRegex = getProperty("table.constantColumns.splitRegex", ",")
      
      if (namesStr.nonEmpty && valuesStr.nonEmpty) {
        val names = namesStr.split(splitRegex).map(_.trim).filter(_.nonEmpty)
        val values = valuesStr.split(splitRegex).map(_.trim).filter(_.nonEmpty)
        
        if (names.length != values.length) {
          throw new IllegalArgumentException(
            s"Constant column names (${names.length}) and values (${values.length}) count must match. " +
            s"Names: ${names.mkString(", ")}, Values: ${values.mkString(", ")}"
          )
        }
        
        names.zip(values).toMap
      } else {
        Map.empty[String, String]
      }
    }
    
    TableConfig(
      sourceKeyspace = getProperty("table.source.keyspace"),
      sourceTable = getProperty("table.source.table"),
      targetSchema = getProperty("table.target.schema", "public"),
      targetTable = getProperty("table.target.table"),
      columnMapping = columnMapping,
      typeMapping = typeMapping,
      primaryKey = primaryKey,
      validate = getBooleanProperty("table.validate", true),
      constantColumns = constantColumns
    )
  }
  
  /**
   * Check if table configuration exists in properties
   */
  def hasTableConfig(props: Properties): Boolean = {
    props.containsKey("table.source.keyspace") && props.containsKey("table.source.table")
  }
}

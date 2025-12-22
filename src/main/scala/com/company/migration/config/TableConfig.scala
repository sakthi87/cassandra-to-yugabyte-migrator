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
  validate: Boolean
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
    
    TableConfig(
      sourceKeyspace = getProperty("table.source.keyspace"),
      sourceTable = getProperty("table.source.table"),
      targetSchema = getProperty("table.target.schema", "public"),
      targetTable = getProperty("table.target.table"),
      columnMapping = columnMapping,
      typeMapping = typeMapping,
      primaryKey = primaryKey,
      validate = getBooleanProperty("table.validate", true)
    )
  }
  
  /**
   * Check if table configuration exists in properties
   */
  def hasTableConfig(props: Properties): Boolean = {
    props.containsKey("table.source.keyspace") && props.containsKey("table.source.table")
  }
}

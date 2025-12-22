package com.company.migration.config

import java.io.InputStream
import java.util.Properties
import org.slf4j.LoggerFactory

/**
 * Loads configuration from a single properties file
 */
object ConfigLoader {
  private val logger = LoggerFactory.getLogger(getClass)
  
  /**
   * Load configuration from properties file
   * @param propertiesPath Path to properties file (default: migration.properties)
   * @return Properties object
   */
  def load(propertiesPath: String = "migration.properties"): Properties = {
    logger.info(s"Loading configuration from: $propertiesPath")
    
    val props = new Properties()
    
    // Try to load from file system first (for external properties file)
    val file = new java.io.File(propertiesPath)
    val inputStream: InputStream = if (file.exists() && file.isFile) {
      logger.info(s"Loading properties from file system: ${file.getAbsolutePath}")
      new java.io.FileInputStream(file)
    } else {
      // Fall back to classpath resource
      logger.info(s"Loading properties from classpath: $propertiesPath")
      Option(getClass.getClassLoader.getResourceAsStream(propertiesPath))
        .getOrElse(throw new IllegalArgumentException(
          s"Properties file not found: $propertiesPath (checked file system and classpath)"
        ))
    }
    
    try {
      props.load(inputStream)
      
      // Replace ${timestamp} placeholder with actual timestamp
      val timestamp = System.currentTimeMillis() / 1000
      props.stringPropertyNames().forEach { key =>
        val value = props.getProperty(key)
        if (value != null && value.contains("${timestamp}")) {
          props.setProperty(key, value.replace("${timestamp}", timestamp.toString))
        }
      }
      
      logger.info(s"Configuration loaded successfully (${props.size()} properties)")
      props
    } finally {
      inputStream.close()
    }
  }
}

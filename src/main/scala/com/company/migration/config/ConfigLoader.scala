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
    val inputStream: InputStream = Option(getClass.getClassLoader.getResourceAsStream(propertiesPath))
      .getOrElse(throw new IllegalArgumentException(s"Properties file not found: $propertiesPath"))
    
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
      
      logger.info("Configuration loaded successfully")
      props
    } finally {
      inputStream.close()
    }
  }
}

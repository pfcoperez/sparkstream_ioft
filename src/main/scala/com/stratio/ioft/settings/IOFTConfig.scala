package com.stratio.ioft.settings

import com.typesafe.config.ConfigFactory

object IOFTConfig {
  val ioftSparkConfig = "ioft.spark.config"
  val ioftSparkStreamingConfig = "ioft.spark.streaming"
  val ioftSourceConfig = "ioft.source"
  val ioftEsConfig = "ioft.es"
  val ioftCassandraConfig = "ioft.cassandra"
}

trait IOFTConfig {

  import IOFTConfig._

  private val config = ConfigFactory.load("settings.conf")

  lazy val sparkConfig = config.getConfig(ioftSparkConfig)

  lazy val sparkStreamingConfig = config.getConfig(ioftSparkStreamingConfig)

  lazy val sourceConfig = config.getConfig(ioftSourceConfig)

  lazy val esConfig = config.getConfig(ioftEsConfig)

  lazy val cassandraConfig = config.getConfig(ioftCassandraConfig)

}

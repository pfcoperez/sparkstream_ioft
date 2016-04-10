package com.stratio.ioft.settings

import com.typesafe.config.ConfigFactory

object IOFTConfig {
  val ioftEsConfig = "ioft.es"
}

trait IOFTConfig {

  import IOFTConfig._

  private val config = ConfigFactory.load("settings.conf")

  lazy val esConfig = config.getConfig(ioftEsConfig)

}

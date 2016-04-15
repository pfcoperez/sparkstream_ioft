package com.stratio.ioft.persistence

import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import com.stratio.ioft.domain.LibrePilot
import com.stratio.ioft.settings.IOFTConfig
import org.apache.spark.streaming.dstream.DStream
import org.elasticsearch.common.settings.Settings

import scala.collection.JavaConversions._

object ESPersistence extends IOFTConfig {

  val settings = Settings.settingsBuilder()

  esConfig.entrySet().map {
    entry =>
      settings.put(entry.getKey, esConfig.getAnyRef(entry.getKey))
  }

  val client = ElasticClient.local(settings.build)

  def persist(dStream: DStream[(String, String)]): Unit ={
    dStream.foreachRDD{ rdd =>
      client.execute{ index into "ioft" / "drone" fields rdd.collect()}
    }
  }

}

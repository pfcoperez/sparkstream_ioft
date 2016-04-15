package com.stratio.ioft

import com.stratio.ioft.domain.LibrePilot.Entry
import com.stratio.ioft.persistence.ESPersistence._
import com.stratio.ioft.serialization.json4s.librePilotSerializers
import com.stratio.ioft.settings.IOFTConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

object StreamDriver extends App with IOFTConfig {

  val conf = new SparkConf() setMaster(sparkConfig.getString("master")) setAppName(sparkConfig.getString("appName"))

  val sc = new StreamingContext(conf, Seconds(sparkStreamingConfig.getLong("batchDuration")))

  val rawInputStream = sc.socketTextStream(sourceConfig.getString("host"), sourceConfig.getInt("port"))

  // Extract case class instances from the input text

  implicit val formats = DefaultFormats ++ librePilotSerializers

  val entriesStream = rawInputStream.map { json =>
    parse(json).extract[Entry]
  }

  entriesStream.print()

  /**
    * TODO: Add the proper logic.
    * DISCLAIMER: I know this is a stupid method and it's wrong.
    */
  def processEntry(entriesStream: DStream[Entry]): DStream[(String, String)] = {
    entriesStream.map {
      entry =>
        (entry.name, entry.id)
    }
  }

  persist(processEntry(entriesStream))

  sc.start()
  sc.awaitTermination

}

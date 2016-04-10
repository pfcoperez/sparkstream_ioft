package com.stratio.ioft

import com.stratio.ioft.domain.LibrePilot.Entry
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.jackson.JsonMethods._
import com.stratio.ioft.serialization.json4s.librePilotSerializers
import org.json4s.DefaultFormats

trait StreamDriverConf {

  val batchDuration = Seconds(1)

  object sourceConf {
    val addr = "localhost"
    val port = 7891
  }

}

object StreamDriver extends App with StreamDriverConf {

  val conf = new SparkConf() setMaster("local[2]") setAppName("Drone stream sample")

  val sc = new StreamingContext(conf, batchDuration)

  val rawInputStream = sc.socketTextStream(sourceConf.addr, sourceConf.port)

  // Extract case class instances from the input text

  implicit val formats = DefaultFormats ++ librePilotSerializers

  val entriesStream = rawInputStream.map { json =>
    parse(json).extract[Entry]
  }

  entriesStream.print()

  sc.start()
  sc.awaitTermination

}

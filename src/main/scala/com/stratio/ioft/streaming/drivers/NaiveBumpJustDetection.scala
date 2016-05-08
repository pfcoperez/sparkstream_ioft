package com.stratio.ioft.streaming.drivers

import com.stratio.ioft.StreamDriver._
import com.stratio.ioft.domain.LibrePilot.Entry
import com.stratio.ioft.domain._
import com.stratio.ioft.domain.measures.Acceleration
import com.stratio.ioft.streaming.transformations.Detectors._
import com.stratio.ioft.streaming.transformations.Sources._
import com.stratio.ioft.util.PresentationTools
import com.stratio.ioft.serialization.json4s._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

object NaiveBumpJustDetection extends App with PresentationTools {
  import org.apache.log4j.{Level, Logger}

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val conf = new SparkConf() setMaster(sparkConfig.getString("master")) setAppName(sparkConfig.getString("appName"))

  val sc = new StreamingContext(conf, Milliseconds(sparkStreamingConfig.getLong("batchDuration")))

  //sc.sparkContext.setLogLevel("ERROR")

  val droneId: DroneIdType = "drone01"

  val rawInputStream = sc.socketTextStream(
    sourceConfig.getString("host"), sourceConfig.getInt("port")
  ) map (json => droneId -> json)

  // Extract case class instances from the input text

  implicit val formats = DefaultFormats ++ librePilotSerializers

  val bumpInterval = Seconds(5)

  val entriesStream = rawInputStream.mapValues(parse(_).extract[Entry])

  val zAccelStream = accelerationStream(entriesStream).map {
    case (did, (ts, Acceleration(_, _, zaccel))) => (did, ts -> zaccel)
  }

  val bumpStream = naiveBumpDetector(zAccelStream)

  bumpStream.foreachRDD(_.foreach(event => (printDroneEvent("BUMP DETECTED", event))))

  sc.start()
  sc.awaitTermination
}

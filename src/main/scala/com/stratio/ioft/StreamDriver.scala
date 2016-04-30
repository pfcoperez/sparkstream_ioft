package com.stratio.ioft

import com.stratio.ioft.domain.DroneIdType
import com.stratio.ioft.domain.LibrePilot.Entry
import com.stratio.ioft.domain.measures.Acceleration

import com.stratio.ioft.streaming.transformations._
import Detectors._
import Aggregators._
import Combinators._
import Sources._

import com.stratio.ioft.serialization.json4s.librePilotSerializers
import com.stratio.ioft.settings.IOFTConfig

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._


object StreamDriver extends App with IOFTConfig {

  import org.apache.log4j.Logger
  import org.apache.log4j.Level

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val conf = new SparkConf() setMaster(sparkConfig.getString("master")) setAppName(sparkConfig.getString("appName"))

  val sc = new StreamingContext(conf, Seconds(sparkStreamingConfig.getLong("batchDuration")))

  //sc.sparkContext.setLogLevel("ERROR")

  val droneId: DroneIdType = "drone01"

  val rawInputStream = sc.socketTextStream(
    sourceConfig.getString("host"), sourceConfig.getInt("port")
  ) map (json => droneId -> json)

  // Extract case class instances from the input text

  implicit val formats = DefaultFormats ++ librePilotSerializers

  val entriesStream = rawInputStream.mapValues(parse(_).extract[Entry])
  val entries5sWindowedStream = entriesStream.window(Seconds(5), Seconds(5))

  val accel5sWindowedStream = accelerationStream(entries5sWindowedStream)
  val hAttitudesin5sWindowedStream = attitudeHistoryStream(attitudeStream(entries5sWindowedStream))

  val normalizedAccel5sWindowedStream:  DStream[(DroneIdType, (BigInt, Acceleration))] =
    normalizedAccelerationStream(accel5sWindowedStream, hAttitudesin5sWindowedStream)

  val bumpStream = averageOutlierBumpDetector(
    normalizedAccel5sWindowedStream.mapValues { case (ts, Acceleration(x,y,z)) => ts -> z }, 5.0, 1.0
  )
  //val bumpStream = averageOutlierBumpDetector(accel5sWindowedStream.mapValues { case (ts, Acceleration(x,y,z)) => ts -> z }, 5.0)
  //val bumpStream = naiveBumpDetector(accelStream)

  bumpStream.foreachRDD(_.foreach(x => println(s"PEAK!!$x")))
  //normalizedAccel5sWindowedStream.foreachRDD(_.foreach(x => println(x)))

  val desiredAttitude = desiredAttitudeStream(entriesStream)
  desiredAttitude.foreachRDD(_.foreach(x => println(s"Desired Attitude: $x")))


  /*
  /**
    * TODO: Add the proper logic.
    * DISCLAIMER: I know this is a stupid method and it's wrong.
    */
  def processEntry(entriesStream: DStream[Entry]): DStream[(String, BigInt, List[Field])] = {
    entriesStream.map {
      entry =>
        (entry.name, entry.gcs_timestamp_ms, entry.fields)
    }
  }

  persist(processEntry(entriesStream))*/

  sc.start()
  sc.awaitTermination

}

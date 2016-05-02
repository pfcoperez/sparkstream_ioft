package com.stratio.ioft

<<<<<<< HEAD
import com.stratio.ioft.domain.LibrePilot.{Entry, Field, Value}
import com.stratio.ioft.persistence.CassandraPersistence._
import com.stratio.ioft.serialization.json4s.librePilotSerializers
import com.stratio.ioft.settings.IOFTConfig
import com.stratio.ioft.Detectors._
import com.stratio.ioft.persistence.PrimaryKey
=======
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

import scala.collection.immutable.ListMap


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

  val bumpInterval = Seconds(5)

  val entriesStream = rawInputStream.mapValues(parse(_).extract[Entry])
  val entries5sWindowedStream = entriesStream.window(bumpInterval, bumpInterval)

  val accel5sWindowedStream = accelerationStream(entries5sWindowedStream)
  val hAttitudesin5sWindowedStream = attitudeHistoryStream(attitudeStream(entries5sWindowedStream))

  val normalizedAccel5sWindowedStream:  DStream[(DroneIdType, (BigInt, Acceleration))] =
    normalizedAccelerationStream(accel5sWindowedStream, hAttitudesin5sWindowedStream)

  val bumpStream = averageOutlierBumpDetector(
    normalizedAccel5sWindowedStream.mapValues { case (ts, Acceleration(x,y,z)) => ts -> z }, 5.0, 1.0
  )
  //val bumpStream = averageOutlierBumpDetector(accel5sWindowedStream.mapValues { case (ts, Acceleration(x,y,z)) => ts -> z }, 5.0)
  //val bumpStream = naiveBumpDetector(accelStream)

  /*
  val colNames = Array("droneID", "event_time", "axis_accel")

  bumpStream.map(peak => (peak._1, peak._2._1, peak._2._2)).foreachRDD(rdd => {
    createTable(
      "peaks",
      Array("DroneID", "event_time", "axis_accel"),
      PrimaryKey(Array("DroneID"), Array("event_time")),
      rdd.take(1).head)
    rdd.foreach(x => {
      println(s"PEAK!!$x")
      persist("peaks", colNames, x)
  })})
  */

  bumpStream.foreachRDD(_.foreach(x => println(s"PEAK!!$x")))
  //accelStream.foreachRDD(_.foreach(x => println(x)))

  val groupedBumps = bumpStream map { case (id, (ts, accel)) => (id, ts/2000) -> (ts, accel) } reduceByKey { (a, b) =>
    Seq(a, b).maxBy(contester => math.abs(contester._2))
  }

  //normalizedAccel5sWindowedStream.foreachRDD(_.foreach(x => println(x)))

  groupedBumps.foreachRDD(_.foreach(x => println(s"PEAK!!$x")))

  val desiredAttitude = desiredAttitudeStream(entriesStream)
  //desiredAttitude.foreachRDD(_.foreach(x => println(s"Desired Attitude: $x")))


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

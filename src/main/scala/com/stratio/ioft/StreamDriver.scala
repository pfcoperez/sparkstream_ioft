package com.stratio.ioft

import com.stratio.ioft.domain.DroneIdType
import com.stratio.ioft.domain.LibrePilot.Entry
import com.stratio.ioft.domain.measures.Acceleration
import com.stratio.ioft.persistence.CassandraPersistence._
import com.stratio.ioft.persistence.PrimaryKey
import com.stratio.ioft.serialization.json4s.librePilotSerializers
import com.stratio.ioft.settings.IOFTConfig
import com.stratio.ioft.streaming.transformations.Aggregators._
import com.stratio.ioft.streaming.transformations.Combinators._
import com.stratio.ioft.streaming.transformations.Detectors._
import com.stratio.ioft.streaming.transformations.Sources._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._


object StreamDriver extends App with IOFTConfig {

  import org.apache.log4j.{Level, Logger}

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


  val bumpColNames = Array("droneID", "event_time", "axis_accel")

  val bumpTableName = "peaks"

  bumpStream.map(peak => (peak._1, peak._2._1, peak._2._2)).foreachRDD(rdd => {
    if (rdd.isEmpty){
      println("No pattern found")
    } else {
      createTable(
        bumpTableName,
        bumpColNames,
        PrimaryKey(Array("DroneID"), Array("event_time")),
        rdd.take(1).head)
      rdd.foreach(x => {
        println(s"PEAK!!$x")
        persist(bumpTableName, bumpColNames, x)
      })
    }
  })


  //bumpStream.foreachRDD(_.foreach(x => println(s"PEAK!!$x")))
  //accelStream.foreachRDD(_.foreach(x => println(x)))

  val groupedBumps = bumpStream map { case (id, (ts, accel)) => (id, ts/2000) -> (ts, accel) } reduceByKey { (a, b) =>
    Seq(a, b).maxBy(contester => math.abs(contester._2))
  }

  //normalizedAccel5sWindowedStream.foreachRDD(_.foreach(x => println(x)))

  val groupedBumpColNames = Array("droneID", "event_time", "axis_accel")

  val groupedBumpTableName = "groupedpeaks"

  groupedBumps.foreachRDD(_.foreach(x => println(s"GROUPED PEAK!!$x")))
  groupedBumps.map(peak => (peak._1._1, peak._2._1, peak._2._2)).foreachRDD(rdd => {
    if (rdd.isEmpty){
      println("No pattern found")
    } else {
      createTable(
        groupedBumpTableName,
        groupedBumpColNames,
        PrimaryKey(Array("DroneID"), Array("event_time")),
        rdd.take(1).head)
      rdd.foreach(x => {
        println(s"GROUPED PEAK!!$x")
        persist(groupedBumpTableName, groupedBumpColNames, x)
      })
    }
  })

  val desiredAttitude = desiredAttitudeStream(entriesStream)
  //desiredAttitude.foreachRDD(_.foreach(x => println(s"Desired Attitude: $x")))


  sc.start()
  sc.awaitTermination

}

package com.stratio.ioft

import com.stratio.ioft.domain.DroneIdType
import com.stratio.ioft.domain.LibrePilot.Entry
import com.stratio.ioft.domain.measures.{Acceleration, Attitude}
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
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._


object StreamDriver extends App with IOFTConfig {

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

  val desiredAttdStream = desiredAttitudeStream(entriesStream)
  val desiredAndActualAttds = desiredAndActualAttitudeStream(desiredAttdStream, attitudeStream(entriesStream), 250)

  desiredAndActualAttds.foreachRDD(_.foreach {
    case (_, (ts, Attitude(_, pitchd, _), Attitude(_, pitcha, _))) /*if(pitchd != pitcha)*/ =>
      println(s"DESIRED vs ACTUAL: ($ts, $pitchd, $pitcha)")
    case _ =>
  })

  val bumpTableName = "peaks"

  val bumpColNames = Array("droneID", "event_time", "axis_accel")

  bumpStream.map(peak => (peak._1, peak._2._1, peak._2._2)).foreachRDD(rdd => {
    if (rdd.isEmpty){
      println("No peak found")
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


  //bumpStream.foreachRDD(_.foreach(x => println(s"PEAK!! $x")))
  //accelStream.foreachRDD(_.foreach(x => println(x)))

  val groupedBumps = bumpStream map { case (id, (ts, accel)) => (id, ts/2000) -> (ts, accel) } reduceByKey { (a, b) =>
    Seq(a, b).maxBy(contester => math.abs(contester._2))
  }

  //normalizedAccel5sWindowedStream.foreachRDD(_.foreach(x => println(x)))

  val groupedBumpTableName = "groupedpeaks"

  val groupedBumpColNames = Array("droneID", "event_time", "axis_accel")

  groupedBumps.foreachRDD(_.foreach(x => println(s"GROUPED PEAK!!$x")))
  groupedBumps.map(peak => (peak._1._1, peak._2._1, peak._2._2)).foreachRDD(rdd => {
    if (rdd.isEmpty){
      println("No grouped peak found")
    } else {
      createTable(
        groupedBumpTableName,
        groupedBumpColNames,
        PrimaryKey(Array("DroneID"), Array("event_time")),
        rdd.take(1).head)
      rdd.foreach {
        case (dId, ts: BigInt, zaccel) =>
          val x = (dId, (ts/1500)*1500, zaccel)
          println(s"GROUPED PEAK!! $x")
          persist(groupedBumpTableName, groupedBumpColNames, x)
      }
    }
  })


  val desiredAttitude = desiredAttitudeStream(entriesStream)
  //desiredAttitude.foreachRDD(_.foreach(x => println(s"Desired Attitude: $x")))

  val desiredTableName = "desiredattitude"

  val desiredColNames = Array("droneID", "event_time", "pitch", "roll", "yaw")

  desiredAttitude.map(att => (att._1, att._2._1, att._2._2.pitch, att._2._2.roll, att._2._2.yaw)).foreachRDD(rdd => {
    if (rdd.isEmpty){
      println("No desired attitude found")
    } else {
      createTable(
        desiredTableName,
        desiredColNames,
        PrimaryKey(Array("DroneID"), Array("event_time")),
        rdd.take(1).head)
      rdd.foreach(x => {
        println(s"DESIRED ATTITUDE!! $x")
        persist(desiredTableName, desiredColNames, x)
      })
    }
  })


  val actualAttitude = attitudeStream(entriesStream)
  //actualAttitude.foreachRDD(_.foreach(x => println(s"Desired Attitude: $x")))

  val actualTableName = "actualattitude"

  val actualColNames = Array("droneID", "event_time", "pitch", "roll", "yaw")

  actualAttitude.map(att => (att._1, att._2._1, att._2._2.pitch, att._2._2.roll, att._2._2.yaw)).foreachRDD(rdd => {
    if (rdd.isEmpty){
      println("No actual attitude found")
    } else {
      createTable(
        actualTableName,
        actualColNames,
        PrimaryKey(Array("DroneID"), Array("event_time")),
        rdd.take(1).head)
      rdd.foreach(x => {
        println(s"ACTUAL ATTITUDE!! $x")
        persist(actualTableName, actualColNames, x)
      })
    }
  })

  val threshold = 20
  val correctedAttitude = desiredAttitude.join(actualAttitude).filter(row =>
    (row._2._2._1.longValue - row._2._1._1.longValue) < threshold
  )
  correctedAttitude.foreachRDD(_.foreach(x => println(s"Corrected Attitude: $x")))

  val acceleration = accelerationStream(entriesStream)
  //acceleration.foreachRDD(_.foreach(x => println(s"Acceleration: $x")))

  val accelerationTableName = "acceleration"

  val accelerationColNames = Array("droneID", "event_time", "x", "y", "z")

  acceleration.map(att => (att._1, att._2._1, att._2._2.x, att._2._2.y, att._2._2.z)).foreachRDD(rdd => {
    if (rdd.isEmpty){
      println("No acceleration found")
    } else {
      createTable(
        accelerationTableName,
        accelerationColNames,
        PrimaryKey(Array("DroneID"), Array("event_time")),
        rdd.take(1).head)
      rdd.foreach(x => {
        println(s"ACCELERATION!! $x")
        persist(accelerationTableName, accelerationColNames, x)
      })
    }
  })


  sc.start()
  sc.awaitTermination

}

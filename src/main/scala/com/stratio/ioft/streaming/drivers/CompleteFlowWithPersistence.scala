package com.stratio.ioft.streaming.drivers

import com.stratio.ioft.domain.DroneIdType
import com.stratio.ioft.domain.LibrePilot.Entry
import com.stratio.ioft.domain.measures.BatteryState.{CurrentState, Stats}
import com.stratio.ioft.domain.measures.{Acceleration, BatteryState}
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


object CompleteFlowWithPersistence extends App with IOFTConfig {

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

  val bumpInterval = Seconds(2)

  val entriesStream = rawInputStream.mapValues(parse(_).extract[Entry])
  val entriesWindowedStream = entriesStream.window(bumpInterval, bumpInterval)

  val accelWindowedStream = accelerationStream(entriesWindowedStream)
  val hAttitudesinWindowedStream = attitudeHistoryStream(attitudeStream(entriesWindowedStream))

  val normalizedAccelWindowedStream:  DStream[(DroneIdType, (BigInt, Acceleration))] =
    normalizedAccelerationStream(accelWindowedStream, hAttitudesinWindowedStream)

  val bumpStream = averageOutlierBumpDetector(
    normalizedAccelWindowedStream.mapValues { case (ts, Acceleration(x,y,z)) => ts -> z }, 5.0, 1.0
  )

  val bumpTableName = "peaks"

  val bumpColNames = Array("droneID", "event_time", "axis_accel")

  bumpStream.map(peak => (peak._1, peak._2._1, peak._2._2)).foreachRDD(rdd => {
    if (rdd.isEmpty){
      //println("No peak found")
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

  val groupedBumps = bumpStream map { case (id, (ts, accel)) => (id, ts/2000) -> (ts, accel) } reduceByKey { (a, b) =>
    Seq(a, b).maxBy(contester => math.abs(contester._2))
  }

  val groupedBumpTableName = "groupedpeaks"

  val groupedBumpColNames = Array("droneID", "event_time", "axis_accel")

  groupedBumps.foreachRDD(_.foreach(x => println(s"GROUPED PEAK!!$x")))
  groupedBumps.map(peak => (peak._1._1, peak._2._1, peak._2._2)).foreachRDD(rdd => {
    if (rdd.isEmpty){
      //println("No grouped peak found")
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

  val desiredTableName = "desiredattitude"

  val desiredColNames = Array("droneID", "event_time", "pitch", "roll", "yaw")

  desiredAttitude.map(att => (att._1, att._2._1, att._2._2.pitch, att._2._2.roll, att._2._2.yaw)).foreachRDD(rdd => {
    if (rdd.isEmpty){
      //println("No desired attitude found")
    } else {
      createTable(
        desiredTableName,
        desiredColNames,
        PrimaryKey(Array("DroneID"), Array("event_time")),
        rdd.take(1).head)
      rdd.foreach(x => {
        //println(s"DESIRED ATTITUDE!! $x")
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
      //println("No actual attitude found")
    } else {
      createTable(
        actualTableName,
        actualColNames,
        PrimaryKey(Array("DroneID"), Array("event_time")),
        rdd.take(1).head)
      rdd.foreach(x => {
        //println(s"ACTUAL ATTITUDE!! $x")
        persist(actualTableName, actualColNames, x)
      })
    }
  })

  val range = 250
  val desiredActualAttitude = desiredAndActualAttitudeStream(desiredAttitude, actualAttitude, range)

  val desiredActualTableName = "desiredactualattitude"

  val desiredActualColNames = Array("droneID", "event_time", "d_pitch", "d_roll", "d_yaw", "a_pitch", "a_roll", "a_yaw")


  desiredActualAttitude.map(att => (att._1, att._2._1, att._2._2.pitch, att._2._2.roll, att._2._2.yaw, att._2._3.pitch, att._2._3.roll, att._2._3.yaw)).foreachRDD(rdd => {
    if (rdd.isEmpty){
      //println("No desired/actual attitude found")
    } else {
      createTable(
        desiredActualTableName,
        desiredActualColNames,
        PrimaryKey(Array("DroneID"), Array("event_time")),
        rdd.take(1).head)
      rdd.foreach(x => {
        println(s"DESIRED/ACTUAL ATTITUDE!! $x")
        persist(desiredActualTableName, desiredActualColNames, x)
      })
    }
  })

  val powerState = powerMonitorStream(entriesStream)

  val powerStateTableName = "power"
  val powerStateColNames = Array("droneID", "event_time",
    "instant_voltage",
    "instant_current",
    "average_current",
    "peak_current",
    "consumed_energy"
  )

  powerState map {
    case (droneId, (ts, bs @ BatteryState(_, currentSt: CurrentState, stats: Stats))) =>
      (Iterator(droneId, ts) ++ currentSt.productIterator ++ stats.productIterator) toArray
  } foreachRDD { powerStateBatch =>

      powerStateBatch.take(1).headOption foreach {
        createTable(
          powerStateTableName,
          powerStateColNames,
          PrimaryKey(Array("DroneID"), Array("event_time")),
          _
        )
      }

      powerStateBatch foreach { bs =>
        persist(powerStateTableName, powerStateColNames, bs.iterator)
      }

  }

  /*
  val acceleration = accelerationStream(entriesStream)
  //acceleration.foreachRDD(_.foreach(x => println(s"Acceleration: $x")))

  val accelerationTableName = "acceleration"

  val accelerationColNames = Array("droneID", "event_time", "x", "y", "z")

  acceleration.map(att => (att._1, att._2._1, att._2._2.x, att._2._2.y, att._2._2.z)).foreachRDD(rdd => {
    if (rdd.isEmpty){
      //println("No acceleration found")
    } else {
      createTable(
        accelerationTableName,
        accelerationColNames,
        PrimaryKey(Array("DroneID"), Array("event_time")),
        rdd.take(1).head)
      rdd.foreach(x => {
        //println(s"ACCELERATION!! $x")
        persist(accelerationTableName, accelerationColNames, x)
      })
    }
  })
  */

  sc.start()
  sc.awaitTermination

}

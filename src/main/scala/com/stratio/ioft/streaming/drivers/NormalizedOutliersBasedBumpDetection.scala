package com.stratio.ioft.streaming.drivers

import com.stratio.ioft.serialization.json4s.librePilotSerializers
import com.stratio.ioft.domain.Entry
import com.stratio.ioft.domain._
import com.stratio.ioft.domain.measures.{Acceleration, Attitude}
import com.stratio.ioft.settings.IOFTConfig
import com.stratio.ioft.streaming.transformations.Aggregators._
import com.stratio.ioft.streaming.transformations.Combinators._
import com.stratio.ioft.streaming.transformations.Detectors._
import com.stratio.ioft.streaming.transformations.Sources._
import com.stratio.ioft.util.PresentationTools
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

object NormalizedOutliersBasedBumpDetection extends App
  with IOFTConfig
  with PresentationTools {

  import org.apache.log4j.{Level, Logger}

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  val conf =
    new SparkConf().
    setMaster(sparkConfig.getString("master"))
      .setAppName(sparkConfig.getString("appName"))
      //.setJars(Array("/home/pfperez/perso/sparkstream_ioft/target/scala-2.10/ioft.jar"))

  val sc = new StreamingContext(conf, Milliseconds(sparkStreamingConfig.getLong("batchDuration")))

  val droneId: DroneIdType = "drone01"

  val rawInputStream = sc.socketTextStream(
    sourceConfig.getString("host"), sourceConfig.getInt("port")
  ) map (json => droneId -> json)

  // Extract case class instances from the input text

  implicit val formats = DefaultFormats ++ librePilotSerializers

  val bumpInterval = Seconds(5)

  val entriesStream = rawInputStream.mapValues { str =>
    println("JSON: " + str)
    val parsed = parse(str)
    println("PARSED JSON: " + parsed)
    parsed.extract[Entry]
    /*Entry(
      List(
        Field("q1", "float32", "", Value("0", 0.6588495373725891) :: Nil),
        Field("q2", "float32", "", Value("0", 0.004345187917351723) :: Nil),
        Field("q3", "float32", "", Value("0", -0.0016269430052489042) :: Nil),
        Field("q4", "float32", "", Value("0", -0.7500156760215759) :: Nil),
        Field("Roll", "float32", "degrees", Value("0", 0.4694768786430359) :: Nil),
        Field("Pitch", "float32", "degrees", Value("0", 0.2506181001663208) :: Nil),
        Field("Yaw", "float32", "degrees", Value("0", -97.40373992919922) :: Nil)
      ),
      BigInt("1459627597025"),
      "D7E0D964",
      0,
      "AttitudeState",
      false
    )*/
  }
  val entriesWindowedStream = entriesStream.window(bumpInterval, bumpInterval)

  val accel5sWindowedStream = accelerationStream(entriesWindowedStream)
  val hAttitudesWindowedStream = attitudeHistoryStream(attitudeStream(entriesWindowedStream))

  val normalizedAccelWindowedStream:  DStream[(DroneIdType, (BigInt, Acceleration))] =
    normalizedAccelerationStream(accel5sWindowedStream, hAttitudesWindowedStream)

  val bumpStream = averageOutlierBumpDetector(
    normalizedAccelWindowedStream.mapValues { case (ts, Acceleration(x,y,z)) => ts -> z }, 0.75, 2.5
  )

  val groupedBumps = bumpStream map { case (id, (ts, accel)) => (id, ts/2000) -> (ts, accel) } reduceByKey { (a, b) =>
    Seq(a, b).maxBy(candidate => math.abs(candidate._2))
  } map {
    case ((id: DroneIdType, _), (ts: TimestampMS, accel: Double)) => (id, ts -> accel)
  }

  groupedBumps.foreachRDD(_.foreach(event => printDroneEvent("BUMP DETECTED", event)))

  sc.start()
  sc.awaitTermination

}

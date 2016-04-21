package com.stratio.ioft

import com.stratio.ioft.domain.LibrePilot.{Entry, Field, Value}
import com.stratio.ioft.persistence.CassandraPersistence._
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

  val rawInputStream = sc.socketTextStream(sourceConfig.getString("host"), sourceConfig.getInt("port"))

  // Extract case class instances from the input text

  implicit val formats = DefaultFormats ++ librePilotSerializers

  val entriesStream = rawInputStream.map { json =>
    parse(json).extract[Entry]
  }

  val accelerationStream = entriesStream.flatMap {
    case Entry(fields: List[Field @ unchecked], ts, _, _, "AccelState", _) =>
      fields collect {
        case Field("z", _, "m/s^2", Value(_, v: Double)::_) => ts -> v
      }
    case _ => Seq()
  }

  val bumpStream = accelerationStream.filter { case (_, v: Double) =>
    -5 <= v && v <= 5
  }

  bumpStream.foreachRDD(_.foreach(x => println(s"PEAK!!$x")))
  accelerationStream.foreachRDD(_.foreach(x => println(x)))

  /*entriesStream.print()

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

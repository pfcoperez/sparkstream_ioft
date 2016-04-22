package com.stratio.ioft

import com.stratio.ioft.domain.LibrePilot.{Entry, Field, Value}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

object Detectors {

  def accelerationStream(entriesStream: DStream[Entry]): DStream[(BigInt, Double)] = {
    entriesStream.flatMap {
      case Entry(fields: List[Field@unchecked], ts, _, _, "AccelState", _) =>
        fields collect {
          case Field("z", _, "m/s^2", Value(_, v: Double) :: _) => ts -> v
        }
      case _ => Seq()
    }
  }

  def naiveBumpDetector(entriesStream: DStream[Entry]): DStream[(BigInt, Double)] =
    accelerationStream(entriesStream).filter { case (_, v: Double) => -5 <= v && v <= 5 }

  def averageOutlierBumpDetector(
                                  entriesStream: DStream[Entry],
                                  threshold: Double = 0.0,
                                  nStDev: Double = 1.8): DStream[(BigInt,
    Double)] =
    accelerationStream(entriesStream).transform { rdd: RDD[(BigInt, Double)] =>
      val accelStats = rdd.map(_._2).stats
      rdd.filter { case (_, accelval) =>
        val diff = Math.abs(accelval - accelStats.mean)
        diff >= Math.max(threshold, nStDev*accelStats.sampleStdev)
      }
    }

}
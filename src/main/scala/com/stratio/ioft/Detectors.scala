package com.stratio.ioft

import com.stratio.ioft.domain.LibrePilot.{Entry, Field, Value}
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.util.StatCounter

object Detectors {

  def accelerationStream(entriesStream: DStream[Entry]): DStream[(BigInt, (Double, Double, Double))] = {
    entriesStream.flatMap {
      case Entry(fields: List[Field@unchecked], ts, _, _, "AccelState", _) =>
        val dimVals = fields collect {
          case Field(dim, _, "m/s^2", Value(_, v: Double) :: _) => dim -> v
        }
        // The three dimensions get ordered by their names: x, y and z and then tupled and combined with timestamp
        dimVals.sortBy(_._1).map(_._2) match {
          case Seq(x, y, z) => Some(ts -> (x, y, z))
          case _ => None
        }
      case _ => Seq()
    }
  }


  def naiveBumpDetector(zAccelStream: DStream[(BigInt, Double)]): DStream[(BigInt, Double)] =
    zAccelStream.filter { case (_, v: Double) => -5 <= v && v <= 5 }

  def averageOutlierBumpDetector(
                                  zAccelStream: DStream[(BigInt, Double)],
                                  threshold: Double = 0.0,
                                  nStDev: Double = 1.8): DStream[(BigInt, Double)] =
    zAccelStream.transform { rdd: RDD[(BigInt, Double)] =>
        if(rdd.isEmpty) rdd
        else {
          val accelStats = rdd.map(_._2).stats
          rdd.filter { case (_, accelval) =>
            val diff = Math.abs(accelval - accelStats.mean)
            diff >= Math.max(threshold, nStDev*accelStats.sampleStdev)
          }
        }
    }

}
package com.stratio.ioft.streaming.transformations

import com.stratio.ioft.domain.DroneIdType
import com.stratio.ioft.domain.LibrePilot.{Entry, Field, Value}
import com.stratio.ioft.domain.measures.{Acceleration, Attitude}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

object Detectors {

  def naiveBumpDetector(
                         zAccelStream: DStream[(DroneIdType, (BigInt, Double))]
                       ): DStream[(DroneIdType, (BigInt, Double))] =
    zAccelStream.filter { case (_, (_, v: Double)) => -5 <= v && v <= 5 }

  def averageOutlierBumpDetector(
                                  zAccelStream: DStream[(DroneIdType, (BigInt, Double))],
                                  threshold: Double = 0.0,
                                  nStDev: Double = 1.8
                                ): DStream[(DroneIdType, (BigInt, Double))] =
    zAccelStream.transform { rdd: RDD[(DroneIdType, (BigInt, Double))] =>
        if(rdd.isEmpty) rdd
        else {
          val accelStats = rdd.map(_._2._2).stats
          rdd.filter { case (_, (_, accelval)) =>
            val diff = Math.abs(accelval - accelStats.mean)
            diff >= Math.max(threshold, nStDev*accelStats.sampleStdev)
          }
        }
    }

}
package com.stratio.ioft.streaming.transformations

import com.stratio.ioft.domain.DroneIdType
import com.stratio.ioft.domain.{Entry, Field, Value}
import com.stratio.ioft.domain.measures.{Acceleration, Attitude}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

object Detectors {

  /**
    * Bump event generator. A bump event will occur whenever z-axis acceleration gets out of [-5,+5] m/s²
    * @param zAccelStream z-axis acceleration event stream
    * @return Filtered z-axis acceleration events: Only those considered bumps.
    */
  def naiveBumpDetector(
                         zAccelStream: DStream[(DroneIdType, (BigInt, Double))]
                       ): DStream[(DroneIdType, (BigInt, Double))] =
    zAccelStream.filter { case (_, (_, v: Double)) => -5 <= v && v <= 5 }

  /**
    * Bumo event generator. A bumo event will occur whenever z-axis acceleration is more than nStDev standard
    * deviations away from the window average AND surpass the given absolute threshold.
    *
    * @param zAccelStream z-axis acceleration event stream windowed by the time frame of interest.
    * @param threshold Minimum ratio of sample to mean to accept a sample as an outlier 
    * @param nStDev How many standard deviations has a z-axis acceleration value to, at least, be afar from the time
    *               frame average.
    * @return Filtered z-axis acceleration events: Only those considered bumps.
    */
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
            val mean = accelStats.mean
            val diff = Math.abs(accelval - mean)
            diff >= Math.max(Math.abs(mean)*threshold, nStDev*accelStats.sampleStdev)
          }
        }
    }

}

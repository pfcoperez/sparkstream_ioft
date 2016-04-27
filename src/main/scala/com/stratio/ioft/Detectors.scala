package com.stratio.ioft

import com.stratio.ioft.domain.DroneIdType
import com.stratio.ioft.domain.LibrePilot.{Entry, Field, Value}
import com.stratio.ioft.domain.measures.{Acceleration, Attitude}
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

object Detectors {

  def attitudeStream(
                      entriesStream: DStream[(DroneIdType, Entry)]
                    ): DStream[(DroneIdType, (BigInt, Attitude))] =
    entriesStream.flatMapValues {
      case Entry(fields: List[Field @ unchecked], ts, _, _, "AttitudeState", _) =>

        val dimVals = fields collect {
          case Field(dim, _, "degrees", Value(_, v: Double) :: _) => dim -> v
        } toMap

        if(dimVals.keySet == Set("Roll", "Pitch", "Yaw"))
          Some(ts -> Attitude(dimVals("Roll"), dimVals("Pitch"), dimVals("Yaw")))
        else None

      case _ => Seq()
    }

  def accelerationStream(
                          entriesStream: DStream[(DroneIdType, Entry)]
                        ): DStream[(DroneIdType, (BigInt, Acceleration))] =
    entriesStream.flatMapValues {

      case Entry(fields: List[Field @ unchecked], ts, _, _, "AccelState", _) =>
        val dimVals = fields collect {
          case Field(dim, _, "m/s^2", Value(_, v: Double) :: _) => dim -> v
        }

        // The three dimensions get ordered by their names: x, y and z and then tupled and combined with timestamp
        dimVals.sortBy(_._1).map(_._2) match {
          case Seq(x, y, z) => Some(ts -> Acceleration(x, y, z))
          case _ => None
        }

      case _ => Seq()
    }


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
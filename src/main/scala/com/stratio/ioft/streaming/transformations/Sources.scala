package com.stratio.ioft.streaming.transformations

import com.stratio.ioft.domain.LibrePilot.{Entry, Field, Value}
import com.stratio.ioft.domain._
import com.stratio.ioft.domain.measures.{Acceleration, Attitude}
import org.apache.spark.streaming.dstream.DStream

object Sources {

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

  def desiredAttitudeStream(
                      entriesStream: DStream[(DroneIdType, Entry)]
                    ): DStream[(DroneIdType, (BigInt, Attitude))] =
    entriesStream.flatMapValues {
      case Entry(fields: List[Field @ unchecked], ts, _, _, "ActuatorDesired", _) =>

        val dimVals = fields collect {
          case Field(dim, _, "%", Value(_, v: Double) :: _) => dim -> v
        } toMap

        import Attitude.{rollRange, pitchRange, yawRange}

        if(Set("Roll", "Pitch", "Yaw") subsetOf dimVals.keySet)
          Some(
            ts -> Attitude(
              rollRange proportionalValue dimVals("Roll"),
              pitchRange proportionalValue  dimVals("Pitch"),
              yawRange proportionalValue  dimVals("Yaw")
            )
          )
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

}

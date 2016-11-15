package com.stratio.ioft.streaming.transformations

import com.stratio.ioft.domain.{Entry, Field, Value}
import com.stratio.ioft.domain._
import com.stratio.ioft.domain.measures.BatteryState._
import com.stratio.ioft.domain.measures.{Acceleration, Attitude, BatteryState}
import org.apache.spark.streaming.dstream.DStream

/**
  * Transformations over input streams aimed to provide a concrete data SOURCES by extracting
  * high level flight details.
  */
object Sources {

  /**
    * Extracts detected (actual) attitude events from the, translated from json, entries stream.
    * @param entriesStream
    * @return Actual attitude events extracted from the low level entries stream.
    */
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


  /**
    * Extracts desired attitude events. Desired means the pilot commands combined with the decision taken by the
    * flight control stabilizer. That is, the attitude the drone should have.
    * @param entriesStream
    * @return Desired attitude events.
    */
  def desiredAttitudeStream(
                             entriesStream: DStream[(DroneIdType, Entry)]
                           ): DStream[(DroneIdType, (BigInt, Attitude))] =
    desiredStream(entriesStream) mapValues { case (ts, attitude, thrust) => (ts, attitude) }

  /**
    * Extracts desired thrust event. Desired means the pilot commands combined with the decision taken by the
    * flight control stabilizer.
    * @param entriesStream
    * @return Desired thrust.
    */
  def desiredThrustStream(
                             entriesStream: DStream[(DroneIdType, Entry)]
                           ): DStream[(DroneIdType, (BigInt, Double))] =
    desiredStream(entriesStream) mapValues { case (ts, attitude, thrust) => (ts, thrust) }


  /**
    * Extracts detected measured acceleration events from the, translated from json, entries stream.
    * @param entriesStream
    * @return Measured acceleration events extracted from the low level entries stream.
    */
  def accelerationStream(
                          entriesStream: DStream[(DroneIdType, Entry)]
                        ): DStream[(DroneIdType, (BigInt, Acceleration))] =
    entriesStream.flatMapValues {

      case Entry(fields: List[Field @ unchecked], ts, _, _, "AccelSensor", _) =>
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

  private def desiredStream(
                             entriesStream: DStream[(DroneIdType, Entry)]
                           ): DStream[(DroneIdType, (BigInt, Attitude, Double))] =
    entriesStream.flatMapValues {
      case Entry(fields: List[Field @ unchecked], ts, _, _, "ActuatorDesired", _) =>

        val dimVals = fields collect {
          case Field(dim, _, "%", Value(_, v: Double) :: _) => dim -> v
        } toMap

        import Attitude.{rollRange, pitchRange, yawRange}


        if(Set("Roll", "Pitch", "Yaw", "Thrust") subsetOf dimVals.keySet)
          Some(
            (
              ts,
              Attitude(
                rollRange proportionalValue dimVals("Roll"),
                pitchRange proportionalValue  dimVals("Pitch"),
                yawRange proportionalValue  dimVals("Yaw")
              ),
              dimVals("Thrust")
            )
          )
        else None

      case _ => Seq()
    }

  def powerMonitorStream(
                      entriesStream: DStream[(DroneIdType, Entry)]
                    ): DStream[(DroneIdType, (BigInt, BatteryState))] =
    entriesStream.flatMapValues {
      case Entry(fields: List[Field @ unchecked], ts, _, _, "FlightBatteryState", _) =>

        val (intDimensions, doubleDimensions) = fields flatMap {
          case Field(dim, _, unit, Value(_, v) :: _) =>
            Some(unit -> v) collect {
              case (unit, v: Double) if Set("A", "V", "mAh") contains unit => false -> (dim -> v)
              case ("", v: Int) => true -> (dim -> v)
            }
        } partition(_._1)

        val intDimMap = (intDimensions map {case (_, (k, v: Int)) => k -> v}).toMap
        val doubleDimMap = (doubleDimensions map {case (_, (k, v: Double)) => k -> v}).toMap

        if(Set(
          "Voltage", "Current", "AvgCurrent",
          "PeakCurrent", "ConsumedEnergy", "NbCells") subsetOf (intDimMap.keySet ++ doubleDimMap.keySet))
          Some(ts ->
            BatteryState(
              intDimMap("NbCells"),
              CurrentState(doubleDimMap("Voltage"), doubleDimMap("Current")),
              Stats(doubleDimMap("AvgCurrent"), doubleDimMap("PeakCurrent"), doubleDimMap("ConsumedEnergy"))
            )
          )
        else None

      case _ => Seq()
    }

}

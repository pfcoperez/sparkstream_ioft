package com.stratio.ioft.streaming.transformations

import com.stratio.ioft.domain._
import com.stratio.ioft.domain.measures.{Acceleration, Attitude}
import com.stratio.ioft.domain.states.AttitudeHistory
import com.stratio.ioft.util.Math.Geometry._
import org.apache.spark.streaming.dstream.DStream
import com.stratio.ioft.domain.measures.VectorMeasure._
import com.stratio.ioft.streaming.transformations.Aggregators.attitudeHistoryStream

import org.apache.spark.streaming.Milliseconds

object Combinators {

  /**
    * Combines historial frame attitude data with acceleration values so acceleration vectors get rotated by their
    * closest attitude angles. This way acceleration vectors reference frame is not the drone itself but earth.
    * @param accelerationStream In a timeframe or window
    * @param attitudeHistoryStream In the very same window than `accelerationStream`.
    * @return
    */
  def normalizedAccelerationStream(
                                    accelerationStream: DStream[(DroneIdType, (BigInt, Acceleration))],
                                    attitudeHistoryStream: DStream[(DroneIdType, AttitudeHistory)]
                                  ): DStream[(DroneIdType, (BigInt, Acceleration))] =
    accelerationStream join attitudeHistoryStream flatMap {
      case (id, ((ts, acceleration), attitudeFrameHistory)) =>
        val closestAttitudes = attitudeFrameHistory.attitudeAt(ts)
        closestAttitudes.headOption map { _ =>
          val (_, attitude: Attitude) = closestAttitudes.minBy {
            case (frame_ts, _) => math.abs((frame_ts-ts).toLong)
          }
          id -> (ts -> Acceleration.tupled(rotate(attitude map (math.toRadians(_)), acceleration))) 
        }
    }

  /**
    * Combines actual and desired actitudes into a single event stream
    *
    * @param desiredAttitudeStream
    * @param attitudeStream
    * @param timeRange
    * @return
    */
  def desiredAndActualAttitudeStream(
                                      desiredAttitudeStream: DStream[(DroneIdType, (BigInt, Attitude))],
                                      attitudeStream: DStream[(DroneIdType, (BigInt, Attitude))],
                                      timeRange: Long
                                    ): DStream[(DroneIdType, (BigInt, Attitude, Attitude))] = {

    def windowed(stream: DStream[(DroneIdType, (BigInt, Attitude))]) = {
      val windowDuration = Milliseconds(timeRange)
      stream.window(windowDuration, windowDuration)
    }

    windowed(desiredAttitudeStream) join attitudeHistoryStream(windowed(attitudeStream)) flatMap {
      case (id, ( (ts, desired), actualAttitudeHistory)) =>
        val closestAttitudes = actualAttitudeHistory.attitudeAt(ts)
        closestAttitudes.headOption map { _ =>
          val (_, actualAttitude: Attitude) = closestAttitudes.minBy {
            case (actual_ts, _) => math.abs((actual_ts-ts).toLong)
          }
          id -> (ts, desired, actualAttitude)
        }
    }

  }

}

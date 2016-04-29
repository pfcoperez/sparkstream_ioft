package com.stratio.ioft.streaming.transformations

import com.stratio.ioft.domain._
import com.stratio.ioft.domain.measures.{Acceleration, Attitude}
import com.stratio.ioft.domain.states.AttitudeHistory
import com.stratio.ioft.util.Math.Geometry._
import org.apache.spark.streaming.dstream.DStream
import com.stratio.ioft.domain.measures.VectorMeasure._

object Combinators {

  def normalizedAccelerationStream(
                                    accelerationStream: DStream[(DroneIdType, (BigInt, Acceleration))],
                                    attitudeHistoryStream: DStream[(DroneIdType, AttitudeHistory)]
                                  ): DStream[(DroneIdType, (BigInt, Acceleration))] =
    accelerationStream.join(attitudeHistoryStream) flatMap {
      case (id, ((ts, acceleration), attitudeFrameHistory)) =>
        val closestAttitudes = attitudeFrameHistory.attitudeAt(ts)
        closestAttitudes.headOption map { _ =>
          val (_, attitude: Attitude) = closestAttitudes.minBy {
            case (frame_ts, _) => math.abs((frame_ts-ts).toLong)
          }
          id -> (ts -> Acceleration.tupled(rotate(attitude map (angle => math.toRadians(-angle)), acceleration)))
        }
    }

}

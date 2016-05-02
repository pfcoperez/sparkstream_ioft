package com.stratio.ioft.domain.measures

object Attitude {
  case class AttitudeRange(maxMagnitude: Double) {
    def rangePercentage(v: Double): Double = v*100.0/maxMagnitude
    def proportionalValue(p: Double): Double = p*maxMagnitude/100.0
    def contains(v: Double): Boolean = math.abs(v) <= maxMagnitude
  }

  val (yawRange, pitchRange, rollRange) = {
    val commonRange = AttitudeRange(180.0)
    (commonRange, commonRange, commonRange)
  }

}

case class Attitude(
                     roll: Double,  // [-180.0, 180.0] Degrees
                     pitch: Double, // [-180.0, 180.0] Degrees
                     yaw: Double    // [-180.0, 180.0] Degrees
                   ) extends VectorMeasure[Double] {
  import Attitude._
  require(
    (yawRange contains yaw) && (pitchRange contains pitch) && (rollRange contains roll)
  )
}

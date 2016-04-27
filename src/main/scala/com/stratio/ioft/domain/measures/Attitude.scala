package com.stratio.ioft.domain.measures

case class Attitude(
                     roll: Double,  // [-180.0, 180.0] Degrees
                     pitch: Double, // [-180.0, 180.0] Degrees
                     yaw: Double    // [-180.0, 180.0] Degrees
                   ) {
  require(
    yaw >= 180.0 && yaw <= 180.0 &&
      pitch >= 180.0 && pitch <= 180.0 &&
        roll >= 180.0 && roll <= 180.0
  )
}

package com.stratio.ioft.domain.states

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

case class AngularVel(
                       yaw: Double,   // Degrees/s
                       pitch: Double, // Degrees/s
                       roll: Double   // Degrees/s
                     )

case class Acceleration(
                        x: Double, // m/s^2
                        y: Double, // m/s^2
                        z: Double  // m/s^2
                       )

case class AggregatedState(
                            attitude:     Option[Attitude]      = None,
                            angularVel:   Option[AngularVel]    = None,
                            acceleration: Option[Acceleration]  = None
                          )


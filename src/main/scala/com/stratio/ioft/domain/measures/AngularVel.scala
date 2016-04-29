package com.stratio.ioft.domain.measures

case class AngularVel(
                       yaw: Double,   // Degrees/s
                       pitch: Double, // Degrees/s
                       roll: Double   // Degrees/s
                     ) extends VectorMeasure[Double]

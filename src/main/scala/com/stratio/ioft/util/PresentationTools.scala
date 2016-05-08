package com.stratio.ioft.util

import java.util.Date

import com.stratio.ioft.domain.{DroneIdType, TimestampMS}

trait PresentationTools {

  def printDroneEvent[T](msg: String, event: (DroneIdType, (TimestampMS, T))): Unit = event match {
    case (did, (ts: TimestampMS, value)) =>
      println(s"$did @ ${new Date(ts.toLong)}:\t$msg:\t$value")
  }

}

package com.stratio.ioft.domain.states

import com.stratio.ioft.domain.measures.Attitude

/**
  * This case class is useful to store the whole history of known attitudes within a window or time frame
  * @param attitudes Collection of attitudes comprising historial attitude data. They should be ordered in time
  */
case class AttitudeHistory(attitudes: Seq[(BigInt, Attitude)]) {
  def isEmpty: Boolean = attitudes.isEmpty

  /**
    * @return Time frame start time.
    */
  def startsAt: Option[BigInt] = attitudes.headOption.map(_._1)

  /**
    * @return Time frame end time.
    */
  def endsAt: Option[BigInt] = attitudes.lastOption.map(_._1)

  /**
    * Tries to locate attitudes at an instant.
    * @param timestamp_ms Instant requested attitude frames should be closest to.
    * @return Attitude frames right before and after the requested timestamp
    */
  def attitudeAt(timestamp_ms: BigInt): Seq[(BigInt, Attitude)] =
    attitudes indexWhere { case (ts, _) => ts > timestamp_ms } match {
      case n if n > 0 => attitudes.slice(n-1, n+1)
      case _ => attitudes.headOption.toSeq
    }

}

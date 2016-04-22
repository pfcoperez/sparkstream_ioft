package com.stratio.ioft.util

object Math {

  type MagnitudeInTime = (BigInt, Double)

  def numericDerivation(f: Seq[MagnitudeInTime]): Seq[MagnitudeInTime] = {
    (f.view zip f.view.drop(1)) map {
      case ((t0, v0), (t1, v1)) => t1 -> (v1 - v0)/(t1 - t0).toLong
    }
  }

}

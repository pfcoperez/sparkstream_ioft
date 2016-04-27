package com.stratio.ioft.domain.states

import com.stratio.ioft.domain.measures.Attitude

case class AttitudeHistory(attitudes: Seq[(BigInt, Attitude)])

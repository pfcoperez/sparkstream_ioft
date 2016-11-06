package com.stratio.ioft.domain.measures

import com.stratio.ioft.domain.measures.BatteryState.{CurrentState, Stats}

object BatteryState {

  case class CurrentState(
                           voltage: Double, // Volts
                           current: Double // Amps
                         )

  case class Stats(
                    averageCurrent: Double, // Amps
                    peakCurrent: Double, // Amps
                    consumedEnergy: Double // mAh
                  )

}

case class BatteryState(numberOfCells: Int, state: CurrentState, stats: Stats)

package com.stratio.ioft.domain.states

import com.stratio.ioft.domain.measures.{Acceleration, AngularVel, Attitude}


case class AggregatedState(
                            attitude:     Option[Attitude]      = None,
                            angularVel:   Option[AngularVel]    = None,
                            acceleration: Option[Acceleration]  = None
                          )

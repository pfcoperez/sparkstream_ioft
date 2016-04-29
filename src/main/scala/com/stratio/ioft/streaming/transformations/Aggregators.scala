package com.stratio.ioft.streaming.transformations

import com.stratio.ioft.domain.DroneIdType
import com.stratio.ioft.domain.measures.Attitude
import com.stratio.ioft.domain.states.AttitudeHistory
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

object Aggregators {

  def attitudeHistoryStream(
                             attitudeStream: DStream[(DroneIdType, (BigInt, Attitude))]
                           ): DStream[(DroneIdType, AttitudeHistory)] =
    attitudeStream.transform { rdd: RDD[(DroneIdType, (BigInt, Attitude))] =>
      rdd.groupByKey.mapValues(atts => AttitudeHistory(atts.toSeq.sortBy(_._1)))
    }

}

package com.stratio.ioft.persistence

import com.datastax.driver.core._
import com.stratio.ioft.domain.LibrePilot
import com.stratio.ioft.settings.IOFTConfig
import org.apache.spark.streaming.dstream.DStream

object CassandraPersistence extends IOFTConfig {

  val cluster = Cluster.builder
    .addContactPoint(cassandraConfig.getString("source.host"))
    .withPort(cassandraConfig.getInt("source.port")).build

  val session = cluster.connect

  def persist(dStream: DStream[(String, BigInt, List[LibrePilot.Field])]) ={
    dStream.foreachRDD {
      rdd =>

    }
    session.execute("CREATE TABLE IF NOT EXISTS ioft")
    val preparedStatement = session.prepare("INSERT INTO ioft.drone () VALUES ();")
    session.executeAsync(preparedStatement.bind())
  }

}


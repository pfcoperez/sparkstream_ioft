package com.stratio.ioft.persistence

import com.datastax.driver.core._
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.stratio.ioft.settings.IOFTConfig

object CassandraPersistence extends IOFTConfig {

  val cluster = Cluster.builder
    .addContactPoint(cassandraConfig.getString("source.host"))
    .withPort(cassandraConfig.getInt("source.port")).build

  val session = cluster.connect

  session.execute("CREATE KEYSPACE IF NOT EXISTS ioft WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}")

  def inferSchema(colNames: Array[String], row: Product): Iterator[String] = {
    for{x <- colNames.iterator; y <- row.productIterator} yield s"$x: ${transformToCqlType(y.getClass)}"
  }

  def transformToCqlType(incomingType: Class[_]): String = {
    incomingType.getSimpleName.toLowerCase match {
      case "bool" => "BOOLEAN"
      case "long" => "BIGINT"
      case "integer" => "INT"
      case other => other.toUpperCase
    }
  }

  def createTable(tableName: String, colNames: Array[String], pk: PrimaryKey, row: Product) = {
    val schema = inferSchema(colNames, row)
    session.execute(s"CREATE TABLE IF NOT EXISTS ioft.$tableName (${schema.mkString}, PRIMARY KEY ($pk))")
  }

  def persist(tableName: String, colNames: Array[String], data: Product) = {
    val array = new Array[Object](colNames.length)
    for(i <- data.productIterator){
      array(array.size) = i.asInstanceOf[Object]
    }
    QueryBuilder.insertInto(tableName).values(colNames, array)
  }

}

case class PrimaryKey(partitionKey: Array[String], clusteringKey: Array[String]){
  override def toString: String = {
    s"(${partitionKey.mkString})${if(clusteringKey.nonEmpty) ","} ${clusteringKey.mkString}"
  }
}


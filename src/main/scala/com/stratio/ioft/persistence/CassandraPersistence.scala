package com.stratio.ioft.persistence

import com.datastax.driver.core._
import com.datastax.driver.core.exceptions.{NoHostAvailableException, QueryExecutionException, QueryValidationException, UnsupportedFeatureException}
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.stratio.ioft.settings.IOFTConfig

import scala.collection.JavaConversions._


object CassandraPersistence extends IOFTConfig {

  val IOFTKeyspace = "ioft"

  val cluster = Cluster.builder
    .addContactPoint(cassandraConfig.getString("source.host"))
    .withPort(cassandraConfig.getInt("source.port")).build

  val session = cluster.connect

  session.execute(s"CREATE KEYSPACE IF NOT EXISTS $IOFTKeyspace WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}")

  //println(s"Current keyspace: ${session.getLoggedKeyspace}")

  session.execute(s"USE $IOFTKeyspace")

  //println(s"Current keyspace: ${session.getLoggedKeyspace}")

  def inferSchema(colNames: Array[String], row: Seq[Any]): Iterator[String] = {
    val y = row.iterator
    for{x <- colNames.iterator} yield {
      s"$x ${transformToCqlType(y.next.getClass)}"
    }
  }

  def transformToCqlType(incomingType: Class[_]): String = {
    incomingType.getSimpleName.toLowerCase match {
      case "string" => "TEXT"
      case "bool" => "BOOLEAN"
      case "long" => "BIGINT"
      case "integer" => "INT"
      case other => other.toUpperCase
    }
  }

  def createTable(tableName: String, colNames: Array[String], pk: PrimaryKey, row: Product): Unit =
    createTable(tableName, colNames, pk, row.productIterator.toSeq)

  def createTable(tableName: String, colNames: Array[String], pk: PrimaryKey, row: Seq[Any]): Unit = {
    val schema = inferSchema(colNames, row)
    try {
      val query = s"CREATE TABLE IF NOT EXISTS $IOFTKeyspace.$tableName (${schema.mkString(", ")}, PRIMARY KEY ($pk))"
      //println(s"Query: $query")
      session.execute(query)
    } catch {
      case nhae: NoHostAvailableException => println(s"${nhae.getCustomMessage(1, true, false)}")
      case qee: QueryExecutionException => println(s"${qee.getMessage}")
      case qve: QueryValidationException => println(s"${qve.getMessage}")
      case other => println(s"Other: $other")
    }

  }

  def adaptToCQLTypes(content: Any): Object = {
    content match {
      case c: BigInt => c.bigInteger.longValue().asInstanceOf[java.lang.Long]
      case other =>
        other.asInstanceOf[Object]
    }
  }


  def persist(tableName: String, colNames: Array[String], data: Product): Unit =
    persist(tableName, colNames, data.productIterator)

  def persist(tableName: String, colNames: Array[String], data: Iterator[Any]): Unit = {
    val array = new Array[Object](colNames.length)
    var pos = 0
    for(i <- data){
      array(pos) = adaptToCQLTypes(i)
      pos+=1
    }
    val statement = QueryBuilder.insertInto(tableName).values(colNames, array)
    try {
      session.execute(statement)
    } catch {
      case nhae: NoHostAvailableException => println(s"${nhae.getCustomMessage(1, true, false)}")
      case qee: QueryExecutionException => println(s"${qee.getMessage}")
      case qve: QueryValidationException => println(s"${qve.getMessage}")
      case ufe: UnsupportedFeatureException => println(s"${ufe.getMessage}")
      case other => println(s"Other: $other")
    }
  }

  def truncate(name: String) = {
    try {
      session.execute(s"TRUNCATE $IOFTKeyspace.$name")
      println(s"Table $IOFTKeyspace.$name truncated")
    } catch {
      case nhae: NoHostAvailableException => println(s"${nhae.getCustomMessage(1, true, false)}")
      case qee: QueryExecutionException => println(s"${qee.getMessage}")
      case qve: QueryValidationException => println(s"${qve.getMessage}")
      case ufe: UnsupportedFeatureException => println(s"${ufe.getMessage}")
      case other => println(s"Other: $other")
    }
  }

  def truncateData() = {
    val ksMetadata = Option(session.getCluster.getMetadata.getKeyspace(IOFTKeyspace))
    ksMetadata match {
      case Some(m) => m.getTables.foreach(t => truncate(t.getName))
      case other => println(s"Keyspace '$IOFTKeyspace' not created")
    }
  }

}

case class PrimaryKey(partitionKey: Array[String], clusteringKey: Array[String]){
  override def toString: String = {
    s"(${partitionKey.mkString})${if(clusteringKey.nonEmpty) ","} ${clusteringKey.mkString}"
  }
}


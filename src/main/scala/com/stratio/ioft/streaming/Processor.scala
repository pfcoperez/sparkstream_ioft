package com.stratio.ioft.streaming

import com.sksamuel.elastic4s.ElasticClient
import com.sksamuel.elastic4s.ElasticDsl._
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

class Processor extends App {

  val client = ElasticClient.local

  val conf = new SparkConf().setMaster("local[*]").setAppName("IOFT")
  val ssc = new StreamingContext(conf, Seconds(5))
  val lines = ssc.socketTextStream("localhost", 7891)
  val words = lines.flatMap(_.split(" "))
  val pairs = words.map(word => (word, 1))
  val wordCounts = pairs.reduceByKey(_ + _)
  wordCounts.print()

  wordCounts.foreachRDD{ rdd =>
    client.execute{ index into "ioft" / "drone" fields rdd.collect()}
  }

  ssc.start()
  ssc.awaitTermination()
}

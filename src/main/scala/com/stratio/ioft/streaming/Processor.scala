package com.stratio.ioft.streaming

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

class Processor extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("IOFT")
  val ssc = new StreamingContext(conf, Seconds(5))
  val lines = ssc.socketTextStream("localhost", 9999)
  val words = lines.flatMap(_.split(" "))
  val pairs = words.map(word => (word, 1))
  val wordCounts = pairs.reduceByKey(_ + _)
  wordCounts.print()
  ssc.start()
  ssc.awaitTermination()
}

package com.stratio.ioft.simulator

import java.io.{DataOutputStream, ObjectOutputStream}
import java.net.{InetAddress, ServerSocket, Socket}
import java.sql.Timestamp
import java.util
import java.util.Map

import com.fasterxml.jackson.core.`type`._
import com.fasterxml.jackson.databind.ObjectMapper

import scala.io.Source

object JsonToSocketSimulator extends App {

  val socketAgent = new SocketAgent()

  var jsonMap = new util.HashMap[String, Object]()
  val mapper = new ObjectMapper
  var prevTimestamp = Long.MaxValue
  var currentTimestamp = Long.MaxValue

  for(line <- Source.fromFile("samples/dronestream_withcontrols.jsons").getLines.zipWithIndex) {
    jsonMap = mapper.readValue(line._1, new TypeReference[Map[String, Object]]() {})
    println(s"LINE ${line._2} = gcs_timestamp_ms: ${jsonMap.get("gcs_timestamp_ms")}")
    currentTimestamp = jsonMap.get("gcs_timestamp_ms").asInstanceOf[Long]
    if(prevTimestamp != Long.MaxValue){
      println(s"Waiting ${currentTimestamp - prevTimestamp} ms")
      Thread.sleep(currentTimestamp - prevTimestamp)
    } else {
      println(s"Waiting 0 ms")
    }
    prevTimestamp = currentTimestamp
    socketAgent.write(line._1)
  }

  socketAgent.close()

}

class SocketAgent {

  val server = new ServerSocket(7891).accept
  println(s"Server Address: ${server.getInetAddress}:${server.getPort}")
  val out = new ObjectOutputStream(new DataOutputStream(server.getOutputStream))

  def write(line: String) = {
    println("Writing to socket")
    out.writeUTF(line)
    println("Flushing")
    out.flush()
  }

  def close() = server.close

}

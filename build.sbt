name := "sparkstream_ioft"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-streaming" % "1.6.1",
  "com.sksamuel.elastic4s" %% "elastic4s-core" % "2.3.0"
  )
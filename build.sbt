name := "sparkstream_ioft"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq (
  "org.apache.spark" %% "spark-streaming" % "1.6.1",
  "com.sksamuel.elastic4s" %% "elastic4s-core" % "2.3.0",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0",
  "org.json4s" %% "json4s-jackson" % "3.3.0",
//  "com.github.nscala-time" %% "nscala-time" % "2.10.0",
  "org.scalactic" %% "scalactic" % "2.2.6",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)

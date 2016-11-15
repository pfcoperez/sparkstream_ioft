name := "sparkstream_ioft"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq (
  "org.apache.spark" %% "spark-streaming" % "1.6.2" % "provided",
//  "com.sksamuel.elastic4s" %% "elastic4s-core" % "2.3.0",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0",
//  "org.json4s" %% "json4s-jackson" % "3.2.10" % "provided",
//  "com.github.nscala-time" %% "nscala-time" % "2.10.0",
//  "org.scalactic" %% "scalactic" % "2.2.6",
//  "com.fasterxml.jackson.core" % "jackson-databind" % "2.3.1",
  //"com.fasterxml.jackson.module" % "jackson-module-scala" % "2.3.1" cross crossMapped("2.9.0" -> "2.9.1", "2.9.0-1"-> "2.9.1", "2.9.1-1" -> "2.9.1"),
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)

mergeStrategy in assembly := {
  case x if x.endsWith("io.netty.versions.properties") => MergeStrategy.first
  case other =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(other)
}


mainClass in assembly := Some("com.stratio.ioft.streaming.drivers.NormalizedOutliersBasedBumpDetection")

assemblyJarName in assembly := "ioft.jar"
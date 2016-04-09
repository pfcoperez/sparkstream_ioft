name := "sparkstream_ioft"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq (
  "org.json4s" %% "json4s-jackson" % "3.3.0",
//  "com.github.nscala-time" %% "nscala-time" % "2.10.0",
  "org.scalactic" %% "scalactic" % "2.2.6",
  "org.scalatest" %% "scalatest" % "2.2.6" % "test"
)

name := "streaming-twitter-test"
organization := "com.revature.song"
version := "0.1"

scalaVersion := "2.12.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "org.apache.spark" %% "spark-mllib" % "3.0.0",
  "org.apache.spark" %% "spark-streaming" % "3.0.0",
  "org.twitter4j" % "twitter4j-core" % "4.0.4",
  "org.twitter4j" % "twitter4j-stream" % "4.0.4",
  //fixed streaming.twitter errors
  // "org.apache.spark" %% "spark-sql-kafka-0-10" % "3.0.1",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0"
)

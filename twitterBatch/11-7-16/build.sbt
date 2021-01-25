name := "batch-top-hashtags"
organization := "com.revature.scalawags.group3"
version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.13.0" % Runtime,
  "org.apache.hadoop" % "hadoop-aws" % "3.2.0",
  "com.amazonaws" % "aws-java-sdk-bundle" % "1.11.375"
)
// libraryDependencies +=

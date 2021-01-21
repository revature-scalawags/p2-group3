scalaVersion := "2.12.10"
name := "hello-world"
organization := "com.revature.mehrab"
version := "1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.0",
  "org.apache.spark" %% "spark-sql" % "3.0.0",
  "org.apache.spark" %% "spark-mllib" % "3.0.0",
  "org.apache.spark" %% "spark-streaming" % "3.0.0",
  "org.twitter4j" % "twitter4j-core" % "4.0.4",
  "org.twitter4j" % "twitter4j-stream" % "4.0.4",
  "org.apache.bahir" %% "spark-streaming-twitter" % "2.4.0"
)

// libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"
// libraryDependencies += "org.apache.httpcomponents" % "httpclient" % "4.5.12"

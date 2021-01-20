scalaVersion := "2.12.10"
name := "top-hashtags"
organization := "ch.epfl.scala"
version := "1.0"
mainClass in (Compile, run) := Some("src.main.scala.HashtagsCountingApp")
mainClass in (Compile, packageBin) := Some("src.main.scala.HashtagsCountingApp")

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"

name := "ScalaExamples"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.0.0",
  "org.apache.spark" %% "spark-sql" % "2.0.0",
  "org.apache.spark" %% "spark-hive" % "2.0.0" % "provided",
  "org.scalatest" %% "scalatest" % "3.0.3" % Test,
  "org.apache.spark" %% "spark-sql" % "2.0.0"
)

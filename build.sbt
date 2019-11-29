scalaVersion := "2.11.8"

name := "clean-data"
organization := "dataproc.codelab"
version := "1.0"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-library" % scalaVersion.value % "provided",
  "org.apache.spark" %% "spark-core" % "2.3.1" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.3.1" % "provided",
  "org.apache.spark" %% "spark-mllib" % "2.3.1" % "provided"
)

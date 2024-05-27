
val sparkVersion = "3.5.1"

lazy val root = project
  .in(file("."))
  .settings(
    name := "datamart",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := "2.12.18"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.3",
  "ch.qos.logback" % "logback-classic" % "1.2.3"
)
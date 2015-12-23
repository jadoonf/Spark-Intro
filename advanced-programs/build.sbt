name := "advanced"

version := "0.1"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.0",
  "org.apache.spark" %% "spark-sql" % "1.5.0"
  "org.apache.spark" %% "spark-streaming" % "1.5.0"
  "org.apache.spark" %% "spark-streaming-kafka_2.10" % "1.5.0"
)

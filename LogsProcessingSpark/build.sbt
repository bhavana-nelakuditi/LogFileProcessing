name := "LogsProcessingSpark"

version := "0.1"

scalaVersion := "2.12.10"

val typesafeConfigVersion = "1.4.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.0.3",
  "org.apache.spark" %% "spark-sql" % "3.0.3",
  "javax.mail" % "mail" % "1.4.7",
  "org.apache.kafka" %% "kafka" % "3.0.0",

  "com.typesafe" % "config" % typesafeConfigVersion
)

// META-INF discarding
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

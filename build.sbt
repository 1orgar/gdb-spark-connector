name := "gdb-spark-connector"

version := "0.5.6"

organization := "io.gdb"

scalaVersion := "2.12.18"

crossScalaVersions := Seq("2.12.18", "2.13.12")

val sparkVersion = "3.5.0"
val arrowVersion = "14.0.2"

libraryDependencies ++= Seq(
  // Spark Dependencies (Provided at runtime in Spark cluster / spark-submit)
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided",

  // Apache Arrow Flight Dependencies for Streaming Ingestion
  "org.apache.arrow" % "arrow-flight"        % arrowVersion,
  "org.apache.arrow" % "arrow-vector"        % arrowVersion,
  "org.apache.arrow" % "arrow-memory-netty"  % arrowVersion,
  "org.apache.arrow" % "arrow-format"        % arrowVersion,

  // Testing
  "org.scalatest" %% "scalatest" % "3.2.17" % Test
)

// Assembly / Fat JAR settings
assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
  case PathList("META-INF", xs @ _*)             => MergeStrategy.discard
  case "reference.conf"                          => MergeStrategy.concat
  case _                                         => MergeStrategy.first
}

assembly / assemblyJarName := s"${name.value}-${version.value}-spark-${sparkVersion}.jar"

scalacOptions ++= Seq(
  "-deprecation",
  "-feature",
  "-unchecked",
  "-Xfatal-warnings"
)

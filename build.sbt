// Versions aligned with the Dataproc 2.2 image (Spark 3.5.x, Scala 2.12.18, Java 11)
ThisBuild / scalaVersion := "2.12.18"
ThisBuild / version      := "1.0"

val sparkVersion = "3.5.3"

lazy val root = (project in file("."))
  .settings(
    name := "earthquake-cooccurrence",
    Compile / mainClass := Some("earthquakes.Main"),
    // Spark is already installed on the Dataproc cluster: "provided" keeps the JAR tiny
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql"  % sparkVersion % "provided"
    ),
    scalacOptions ++= Seq("-deprecation", "-feature", "-release", "11"),
    // allow `sbt run` locally despite the "provided" scope
    Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated,
    Compile / run / fork := true,
    // same --add-opens flags that spark-submit adds on Java 17+
    Compile / run / javaOptions ++= Seq("-Xmx4g") ++ Seq(
      "java.lang", "java.lang.invoke", "java.lang.reflect", "java.io", "java.net", "java.nio",
      "java.util", "java.util.concurrent", "java.util.concurrent.atomic", "sun.nio.ch", "sun.nio.cs",
      "sun.security.action", "sun.util.calendar"
    ).map(p => s"--add-opens=java.base/$p=ALL-UNNAMED")
  )

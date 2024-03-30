val scala3Version = "2.13.0"

lazy val root = project
  .in(file("."))
  .settings(
    name := "hello-world",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.1" % "provided",
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.1" 
)


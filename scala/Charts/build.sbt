ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "Charts"
  )

libraryDependencies += "com.databricks" % "databricks-connect" % "13.3.0"
libraryDependencies += "org.jfree" % "jfreechart" % "1.5.4"

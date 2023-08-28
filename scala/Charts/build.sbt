ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.17"

lazy val root = (project in file("."))
  .settings(
    name := "Charts"
  )

resolvers += "DB Connect Staging" at "https://previewstoragespark.blob.core.windows.net/privatepreviewdbconnect/20230810_DBConnectv2_Scala/dbconnect-staging"
resolvers += "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/"

libraryDependencies += "com.databricks.connect" % "dbconnect" % "13.3.0-SNAPSHOT"
libraryDependencies += "org.jfree" % "jfreechart" % "1.5.4"

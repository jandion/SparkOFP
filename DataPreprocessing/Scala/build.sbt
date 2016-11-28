lazy val datapreprocessing = (project in file(".")).
  settings(
    name := "my-DataPreprocessing",
    version := "1.0",
    scalaVersion := "2.10.5",
    mainClass in Compile := Some("Main")
  )
libraryDependencies += "com.typesafe" % "config" % "1.3.0"

// META-INF discarding
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
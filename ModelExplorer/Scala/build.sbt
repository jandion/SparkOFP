lazy val modelexplorer = (project in file(".")).
  settings(
    name := "my-ModelExplorer",
    version := "1.0",
    scalaVersion := "2.10.5",
    mainClass in Compile := Some("Main")
  )
libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "1.6.0"%"provided",
  "org.apache.spark" %% "spark-mllib" % "1.6.0"%"provided",
  "com.typesafe" % "config" % "1.2.1",
  "com.github.eirslett" %% "sbt-slf4j" % "0.1")

// META-INF discarding
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
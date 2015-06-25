lazy val root = (project in file(".")).
  settings(
    name := "sparkmulticount",
    version := "1.0",
    scalaVersion := "2.10.5",
    assemblyJarName in assembly := "multicounter.jar",

    resolvers ++=  Seq("SnowPlow Repo" at "http://maven.snplow.com/releases/", "Twitter Maven Repo" at "http://maven.twttr.com/"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "1.4.0" % "provided",
      "com.opencsv" % "opencsv" % "3.4",
      "com.snowplowanalytics"  %% "scala-maxmind-iplookups"  % "0.2.0",
      "org.scalatest" %% "scalatest" % "2.2.4" % "test"))


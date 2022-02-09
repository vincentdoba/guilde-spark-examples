lazy val sparkSandbox = (project in file("."))
  .settings(
    name := "spark-guilde-examples",
    scalaVersion := "2.12.15",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "3.2.0",
      "io.delta" %% "delta-core" % "1.0.0",
      "org.scalatest" %% "scalatest" % "3.0.8" % Test
    )
  )

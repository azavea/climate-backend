name := "backend"

libraryDependencies ++= Seq(
  "commons-io" % "commons-io" % "2.6",
  "org.apache.hadoop" % "hadoop-client" % "2.8.0" % "provided",
  "org.apache.spark" %% "spark-core" % "2.2.0" % "provided",
  "org.locationtech.geotrellis" %% "geotrellis-raster" % "1.2.0-RC2",
  "org.locationtech.geotrellis" %% "geotrellis-s3"     % "1.2.0-RC2",
  "org.locationtech.geotrellis" %% "geotrellis-spark"  % "1.2.0-RC2",
  "org.locationtech.geotrellis" %% "geotrellis-vector" % "1.2.0-RC2"
)

fork in Test := false
parallelExecution in Test := false

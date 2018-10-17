name := "backend"

libraryDependencies ++= Seq(
  "commons-io"                   % "commons-io"        % "2.6",
  "org.apache.hadoop"            % "hadoop-client"     % "2.8.0"      % "provided",
  "org.apache.spark"            %% "spark-core"        % "2.2.0"      % "provided",
  "org.locationtech.geotrellis" %% "geotrellis-raster" % "1.2.0",
  "org.locationtech.geotrellis" %% "geotrellis-s3"     % "1.2.0",
  "org.locationtech.geotrellis" %% "geotrellis-spark"  % "1.2.0",
  "org.locationtech.geotrellis" %% "geotrellis-vector" % "1.2.0",

  "com.typesafe.akka"           %% "akka-http-spray-json-experimental" % "2.4.11.2",
  "com.typesafe.akka"           %% "akka-actor"        % "2.5.6",
  "com.typesafe.akka"           %% "akka-slf4j"        % "2.5.6",
  "com.typesafe.akka"           %% "akka-stream"       % "2.5.6",
  "com.typesafe.akka"           %% "akka-http"         % "10.0.10",
  "com.typesafe.akka"           %% "akka-http-testkit" % "10.0.10",
  "ch.megard"                   %% "akka-http-cors"    % "0.2.2"
)

fork in Test := false
parallelExecution in Test := false

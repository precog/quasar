name := "Web"

mainClass in Compile := Some("quasar.api.Server")

libraryDependencies ++= {
  val http4sVersion = "0.8.6"
  Seq(
    "com.github.tototoshi" %% "scala-csv"           % "1.1.2",
    "org.http4s"           %% "http4s-argonaut"     % http4sVersion,
    "org.http4s"           %% "http4s-blazeclient"  % http4sVersion,
    "org.http4s"           %% "http4s-blazeserver"  % http4sVersion,
    "org.http4s"           %% "http4s-dsl"          % http4sVersion,
    "org.scodec"           %% "scodec-scalaz"       % "1.1.0"
  )
}
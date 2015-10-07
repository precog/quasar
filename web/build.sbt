name := "Web"

mainClass in Compile := Some("quasar.api.Server")

val http4sVersion     = "0.10.1"

libraryDependencies ++= Seq(
  "org.http4s"           %% "http4s-dsl"          % http4sVersion % "compile, test",
  "org.http4s"           %% "http4s-argonaut"     % http4sVersion % "compile, test",
  "org.http4s"           %% "http4s-blaze-server" % http4sVersion % "compile, test",
  "com.github.tototoshi" %% "scala-csv"           % "1.1.2",
  "org.scodec"           %% "scodec-scalaz"       % "1.1.0"
)

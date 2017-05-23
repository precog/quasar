libraryDependencies += "org.kohsuke" % "github-api" % "1.59"

disablePlugins(TravisCiPlugin)

scalacOptions ++= scalacOptions_2_10

// sbt/sbt#2572
scalacOptions in (Compile, console) --= Seq(
  "-Yno-imports",
  "-Ywarn-unused-import")

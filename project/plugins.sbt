resolvers += "Jenkins-CI" at "http://repo.jenkins-ci.org/repo"
libraryDependencies += "org.kohsuke" % "github-api" % "1.59"

addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.0.0-M15")

addSbtPlugin("com.eed3si9n"      % "sbt-assembly"    % "0.14.3")
addSbtPlugin("com.eed3si9n"      % "sbt-buildinfo"   % "0.6.1")
addSbtPlugin("de.heikoseeberger" % "sbt-header"      % "1.5.0")
addSbtPlugin("com.jsuereth"      % "sbt-pgp"         % "1.0.0")
addSbtPlugin("com.github.gseitz" % "sbt-release"     % "1.0.3")
addSbtPlugin("org.scoverage"     % "sbt-scoverage"   % "1.3.3")
addSbtPlugin("org.xerial.sbt"    % "sbt-sonatype"    % "1.1")
addSbtPlugin("com.dwijnand"      % "sbt-travisci"    % "1.0.0")
// TODO update to `sbt-wartremover-contrib` when a version for 2.0.2 (or higher) is released
addSbtPlugin("org.wartremover"   % "sbt-wartremover" % "2.0.2")

val commonScalacOptions = Seq(
  "-deprecation",
  "-encoding", "UTF-8",
  "-feature",
  "-language:existentials",
  "-language:higherKinds",
  "-language:implicitConversions",
  "-unchecked",
  "-Xfatal-warnings",
  "-Xfuture",
  "-Yno-adapted-args",
  "-Yno-imports",
  "-Ywarn-dead-code",
  "-Ywarn-numeric-widen",
  "-Ywarn-value-discard")

// NB: This option triggers issues that need to be fixed in the main project.
scalacOptions ++= commonScalacOptions ++ Seq("-Xlint")

buildInfoKeys := Seq[BuildInfoKey]("scalacOptions" -> commonScalacOptions)

buildInfoPackage := "quasar.project.build"

lazy val meta = project.in(file(".")).enablePlugins(BuildInfoPlugin)

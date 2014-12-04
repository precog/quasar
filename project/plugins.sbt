// Temporary workaround for problem resolving scala 2.10.2 jars:
resolvers += Resolver.sonatypeRepo("releases")

// WartRemover
addSbtPlugin("org.brianmckenna" % "sbt-wartremover" % "0.11")

// Scoverage:
resolvers += Classpaths.sbtPluginReleases

addSbtPlugin("org.scoverage" %% "sbt-scoverage" % "0.99.11")

addSbtPlugin("com.sksamuel.scoverage" %% "sbt-coveralls" % "0.0.5")

// sbt-one-jar
addSbtPlugin("org.scala-sbt.plugins" % "sbt-onejar" % "0.8")
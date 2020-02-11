resolvers += Resolver.sonatypeRepo("releases")
resolvers += Resolver.bintrayIvyRepo("slamdata-inc", "sbt-plugins")

addSbtPlugin("com.slamdata" % "sbt-slamdata" % "5.4.0-b066275")

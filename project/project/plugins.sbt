resolvers += Resolver.sonatypeRepo("releases")
resolvers += Resolver.bintrayIvyRepo("slamdata-inc", "sbt-plugins")

addSbtPlugin("com.slamdata" % "sbt-slamdata" % "5.4.0-45f3e27")

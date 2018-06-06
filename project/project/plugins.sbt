resolvers += Resolver.sonatypeRepo("releases")
resolvers += Resolver.bintrayRepo("slamdata-inc", "maven-public")

addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.1.0-M4")
addSbtPlugin("com.slamdata"    % "sbt-slamdata" % "0.8.5")

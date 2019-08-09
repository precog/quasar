resolvers += Resolver.sonatypeRepo("releases")
resolvers += Resolver.bintrayRepo("slamdata-inc", "maven-public")

addSbtPlugin("io.get-coursier" % "sbt-coursier" % "2.0.0-RC3-2")
addSbtPlugin("com.slamdata"    % "sbt-slamdata" % "3.1.0")

resolvers += Resolver.sonatypeRepo("releases")
resolvers += Resolver.bintrayRepo("slamdata-inc", "maven-public")

addSbtPlugin("com.eed3si9n"          % "sbt-buildinfo"     % "0.9.0")
addSbtPlugin("com.slamdata"          % "sbt-slamdata"      % "5.3.3")

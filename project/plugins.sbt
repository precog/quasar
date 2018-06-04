resolvers += Resolver.sonatypeRepo("releases")
resolvers += Resolver.bintrayRepo("slamdata-inc", "maven-public")

addSbtPlugin("com.eed3si9n"          % "sbt-assembly"  % "0.14.5")
addSbtPlugin("com.eed3si9n"          % "sbt-buildinfo" % "0.7.0")
addSbtPlugin("io.get-coursier"       % "sbt-coursier"  % "1.1.0-M1")
addSbtPlugin("com.slamdata"          % "sbt-slamdata"  % "0.8.5")
addSbtPlugin("pl.project13.scala"    % "sbt-jmh"       % "0.2.27")
addSbtPlugin("com.github.romanowski" % "hoarder"       % "1.0.2-RC2")

// don't look, don't look, don't look...
addSbtPlugin("com.codecommit" % "sbt-evil-mode" % "0.1-bdad513")

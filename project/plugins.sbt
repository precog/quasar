resolvers += Resolver.sonatypeRepo("releases")

addSbtPlugin("com.eed3si9n"       % "sbt-assembly"  % "0.14.3")
addSbtPlugin("com.eed3si9n"       % "sbt-buildinfo" % "0.6.1")
addSbtPlugin("io.get-coursier"    % "sbt-coursier"  % "1.0.0-M15")
addSbtPlugin("org.scoverage"      % "sbt-scoverage" % "1.5.0")
addSbtPlugin("com.slamdata"       % "sbt-slamdata"  % "0.4.0")
addSbtPlugin("pl.project13.scala" % "sbt-jmh"       % "0.2.24")

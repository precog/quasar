resolvers += Resolver.sonatypeRepo("releases")

addSbtPlugin("com.eed3si9n"       % "sbt-assembly"  % "0.14.5")
addSbtPlugin("com.eed3si9n"       % "sbt-buildinfo" % "0.7.0")
addSbtPlugin("io.get-coursier"    % "sbt-coursier"  % "1.0.0-RC12")
addSbtPlugin("org.scoverage"      % "sbt-scoverage" % "1.5.1")
addSbtPlugin("com.slamdata"       % "sbt-slamdata"  % "0.5.2")
addSbtPlugin("pl.project13.scala" % "sbt-jmh"       % "0.2.27")

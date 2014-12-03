organization := "com.slamdata.slamengine"

name := "it"

version := "1.1.1-SNAPSHOT"

scalaVersion := "2.11.2"

val scalazVersion     = "7.1.0"
val monocleVersion    = "0.5.0"
val unfilteredVersion = "0.8.1"

fork in run := true

libraryDependencies ++= Seq(
  "org.scalaz"        %% "scalaz-core"               % scalazVersion              % "test",
  "org.scalaz"        %% "scalaz-concurrent"         % scalazVersion              % "test",  
  "org.scalaz.stream" %% "scalaz-stream"             % "0.5a"                     % "test",
  "org.spire-math"    %% "spire"                     % "0.8.2"                    % "test",
  "com.github.julien-truffaut" %% "monocle-core"     % monocleVersion             % "test",
  "com.github.julien-truffaut" %% "monocle-generic"  % monocleVersion             % "test",
  "com.github.julien-truffaut" %% "monocle-macro"    % monocleVersion             % "test",
  "org.threeten"      %  "threetenbp"                % "0.8.1"                    % "test",
  "org.mongodb"       %  "mongo-java-driver"         % "2.12.2"                   % "test",
  "net.databinder"    %% "unfiltered-filter"         % unfilteredVersion          % "test",
  "net.databinder"    %% "unfiltered-netty-server"   % unfilteredVersion          % "test",
  "net.databinder"    %% "unfiltered-netty"          % unfilteredVersion          % "test",
  "io.argonaut"       %% "argonaut"                  % "6.1-M4"                   % "test",
  "org.jboss.aesh"    %  "aesh"                      % "0.55"                     % "test",
  "org.scalaz"        %% "scalaz-scalacheck-binding" % scalazVersion              % "test",
  "com.github.julien-truffaut" %% "monocle-law"      % monocleVersion             % "test",
  "org.scalacheck"    %% "scalacheck"                % "1.10.1"                   % "test",
  "org.specs2"        %% "specs2"                    % "2.3.13-scalaz-7.1.0-RC1"  % "test",
  "net.databinder.dispatch" %% "dispatch-core"       % "0.11.1"                   % "test"
)

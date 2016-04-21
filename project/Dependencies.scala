package quasar.project

import scala.collection.Seq

import sbt._, Keys._

object Dependencies {
  private val scalazVersion  = "7.2.2"
  private val slcVersion     = "0.4"
  private val monocleVersion = "1.2.1"
  private val pathyVersion   = "0.2.0"
  private val http4sVersion  = "0.13.2a"
  private val mongoVersion   = "3.2.2"
  private val nettyVersion   = "4.0.36.Final"
  private val refinedVersion = "0.4.0"
  private val raptureVersion = "2.0.0-M5"

  val core = Seq(
    "org.scalaz"        %% "scalaz-core"               % scalazVersion            % "compile, test" force(),
    "org.scalaz"        %% "scalaz-concurrent"         % scalazVersion            % "compile, test",
    "org.scalaz.stream" %% "scalaz-stream"             % "0.8.1a"                 % "compile, test",
    "com.github.julien-truffaut" %% "monocle-core"     % monocleVersion           % "compile, test",
    "com.github.julien-truffaut" %% "monocle-generic"  % monocleVersion           % "compile, test",
    "com.github.julien-truffaut" %% "monocle-macro"    % monocleVersion           % "compile, test",
    "com.github.scopt"  %% "scopt"                     % "3.4.0"                  % "compile, test",
    "org.threeten"      %  "threetenbp"                % "1.3.1"                  % "compile, test",
    "org.mongodb"       %  "mongodb-driver-async"      % mongoVersion             % "compile, test",
    "io.netty"          %  "netty-buffer"              % nettyVersion             % "compile, test",
    "io.netty"          %  "netty-transport"           % nettyVersion             % "compile, test",
    "io.netty"          %  "netty-handler"             % nettyVersion             % "compile, test",
    "io.argonaut"       %% "argonaut"                  % "6.2-M1"                 % "compile, test",
    "io.argonaut"       %% "argonaut-scalaz"           % "6.2-M1"                 % "compile, test",
    "org.jboss.aesh"    %  "aesh"                      % "0.66.8"                  % "compile, test",
    "org.typelevel"     %% "shapeless-scalaz"          % slcVersion               % "compile, test",
    "com.slamdata"      %% "matryoshka-core"           % "0.9.0"                  % "compile",
    "com.slamdata"      %% "pathy-core"                % pathyVersion             % "compile",
    "com.github.mpilquist" %% "simulacrum"             % "0.7.0"                  % "compile, test",
    "org.http4s"        %% "http4s-core"               % http4sVersion            % "compile",
    "com.github.tototoshi" %% "scala-csv"              % "1.3.1"                  % "compile",
    "com.slamdata"      %% "pathy-scalacheck"          % pathyVersion             % "test",
    "org.scalaz"        %% "scalaz-scalacheck-binding" % scalazVersion            % "test",
    "org.specs2"        %% "specs2-core"               % "3.7.3-scalacheck-1.12"  % "test",
    // Can't upgrade scalacheck until scalaz-scalacheck-binding is upgraded to latest scalacheck which looks
    // like it may never happen: https://github.com/scalaz/scalaz/issues/1096
    "org.scalacheck"    %% "scalacheck"                % "1.12.5"                 % "test" force(),
    "org.typelevel"     %% "scalaz-specs2"             % "0.4.0"                  % "test",
    "org.typelevel"     %% "shapeless-scalacheck"      % slcVersion               % "test",
    "eu.timepit"        %% "refined"                   % refinedVersion           % "compile, test",
    "eu.timepit"        %% "refined-scalacheck"        % refinedVersion           % "test")

  val web = Seq(
    "org.http4s"           %% "http4s-dsl"            % http4sVersion % "compile, test",
    "org.http4s"           %% "http4s-argonaut"       % http4sVersion % "compile, test",
    "org.http4s"           %% "http4s-blaze-server"   % http4sVersion  % "compile, test",
    "org.http4s"           %% "http4s-blaze-client"   % http4sVersion  % "test",
    // This dependency is calling for scalaz 7.1.x which gets evicted. All tests work even though there is a chance
    // of hitting a binary incompatibility at some point.
    // Version 1.2.0 is still using 7.1.x and if I remember correctly, that version does trigger a binary
    // incompatibility when running the tests.
    "org.scodec"           %% "scodec-scalaz"         % "1.1.0",
    "ch.qos.logback"       %  "logback-classic"       % "1.1.7",
    "com.propensive"       %% "rapture-json"          % raptureVersion % "test",
    "com.propensive"       %% "rapture-json-json4s"   % raptureVersion % "test")
}

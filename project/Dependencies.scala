package quasar.project

import scala.collection.Seq

import sbt._, Keys._

object Dependencies {
  private val scalazVersion  = "7.1.6"
  private val slcVersion     = "0.4"
  private val monocleVersion = "1.1.1"
  private val pathyVersion   = "0.0.3"
  private val http4sVersion  = "0.12.4"
  private val mongoVersion   = "3.2.1"
  private val nettyVersion   = "4.0.26.Final"
  private val refinedVersion = "0.3.7"
  private val raptureVersion = "2.0.0-M5"

  val core = Seq(
    // NB: This version is forced because there seems to be some difference
    //     with `Free` or `Catchable` in 7.1.7 that affects our implementation
    //     of `CatchableFree`.
    "org.scalaz"        %% "scalaz-core"               % scalazVersion  % "compile, test" force(),
    "org.scalaz"        %% "scalaz-concurrent"         % scalazVersion  % "compile, test",
    "org.scalaz.stream" %% "scalaz-stream"             % "0.8"          % "compile, test",
    "com.github.julien-truffaut" %% "monocle-core"     % monocleVersion % "compile, test",
    "com.github.julien-truffaut" %% "monocle-generic"  % monocleVersion % "compile, test",
    "com.github.julien-truffaut" %% "monocle-macro"    % monocleVersion % "compile, test",
    "com.github.scopt"  %% "scopt"                     % "3.3.0"        % "compile, test",
    "org.threeten"      %  "threetenbp"                % "1.2"          % "compile, test",
    "org.mongodb"       %  "mongodb-driver-async"      % mongoVersion   % "compile, test",
    "io.netty"          %  "netty-buffer"              % nettyVersion   % "compile, test",
    "io.netty"          %  "netty-transport"           % nettyVersion   % "compile, test",
    "io.netty"          %  "netty-handler"             % nettyVersion   % "compile, test",
    "io.argonaut"       %% "argonaut"                  % "6.1"          % "compile, test",
    "org.jboss.aesh"    %  "aesh"                      % "0.55"         % "compile, test",
    "org.typelevel"     %% "shapeless-scalaz"          % slcVersion     % "compile, test",
    "com.slamdata"      %% "matryoshka-core"           % "0.1.0"        % "compile",
    "com.slamdata"      %% "pathy-core"                % pathyVersion   % "compile",
    "com.github.mpilquist" %% "simulacrum"             % "0.7.0"        % "compile, test",
    "org.http4s"        %% "http4s-core"               % http4sVersion  % "compile",
    "com.github.tototoshi" %% "scala-csv"              % "1.1.2"        % "compile",
    "com.slamdata"      %% "pathy-scalacheck"          % pathyVersion   % "test",
    "org.scalaz"        %% "scalaz-scalacheck-binding" % scalazVersion  % "test",
    "org.specs2"        %% "specs2-core"               % "2.4"          % "test",
    "org.scalacheck"    %% "scalacheck"                % "1.11.6"       % "test" force(),
    "org.typelevel"     %% "scalaz-specs2"             % "0.3.0"        % "test",
    "org.typelevel"     %% "shapeless-scalacheck"      % slcVersion     % "test",
    "eu.timepit"        %% "refined"                   % refinedVersion % "compile, test",
    "eu.timepit"        %% "refined-scalacheck"        % refinedVersion % "test")

  val web = Seq(
    "org.http4s"           %% "http4s-dsl"          % http4sVersion % "compile, test",
    "org.http4s"           %% "http4s-argonaut"     % http4sVersion % "compile, test"
      // TODO: remove once jawn-streamz is in agreement with http4s on
      //       scalaz-stream version
      exclude("org.scalaz.stream", "scalaz-stream_2.11"),
    "org.http4s"           %% "http4s-blaze-server"   % http4sVersion  % "compile, test",
    "org.http4s"           %% "http4s-blaze-client"   % http4sVersion  % "test",
    "org.scodec"           %% "scodec-scalaz"         % "1.1.0",
    "ch.qos.logback"       %  "logback-classic"       % "1.1.3",
    "com.propensive"       %% "rapture-json"          % raptureVersion % "test",
    "com.propensive"       %% "rapture-json-argonaut" % raptureVersion % "test")
}

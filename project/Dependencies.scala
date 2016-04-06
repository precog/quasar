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
    "org.scalaz"        %% "scalaz-core"               % scalazVersion force(),
    "org.scalaz"        %% "scalaz-concurrent"         % scalazVersion,
    "org.scalaz.stream" %% "scalaz-stream"             % "0.8",
    "com.github.julien-truffaut" %% "monocle-core"     % monocleVersion,
    "com.github.julien-truffaut" %% "monocle-generic"  % monocleVersion,
    "com.github.julien-truffaut" %% "monocle-macro"    % monocleVersion,
    "com.github.scopt"  %% "scopt"                     % "3.3.0",
    "org.threeten"      %  "threetenbp"                % "1.2",
    "org.mongodb"       %  "mongodb-driver-async"      % mongoVersion,
    "io.netty"          %  "netty-buffer"              % nettyVersion,
    "io.netty"          %  "netty-transport"           % nettyVersion,
    "io.netty"          %  "netty-handler"             % nettyVersion,
    "io.argonaut"       %% "argonaut"                  % "6.1",
    "org.jboss.aesh"    %  "aesh"                      % "0.55",
    "org.typelevel"     %% "shapeless-scalaz"          % slcVersion,
    "com.slamdata"      %% "matryoshka-core"           % "0.8.0",
    "com.slamdata"      %% "pathy-core"                % pathyVersion,
    "com.github.mpilquist" %% "simulacrum"             % "0.7.0",
    "org.http4s"        %% "http4s-core"               % http4sVersion,
    "com.github.tototoshi" %% "scala-csv"              % "1.1.2",
    "eu.timepit"        %% "refined"                   % refinedVersion)

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

  val scalacheck = Seq(
    "com.slamdata"      %% "pathy-scalacheck"          % pathyVersion,
    "org.scalaz"        %% "scalaz-scalacheck-binding" % scalazVersion,
    "org.specs2"        %% "specs2-core"               % "2.4",
    "org.scalacheck"    %% "scalacheck"                % "1.11.6" force(),
    "org.typelevel"     %% "scalaz-specs2"             % "0.3.0",
    "org.typelevel"     %% "shapeless-scalacheck"      % slcVersion,
    "eu.timepit"        %% "refined-scalacheck"        % refinedVersion)
}

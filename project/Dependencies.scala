package quasar.project

import scala.collection.Seq

import sbt._, Keys._

object Dependencies {
  private val argonautVersion   = "6.2-M3"
  private val http4sVersion     = "0.14.1a"
  private val monocleVersion    = "1.2.2"
  private val nettyVersion      = "4.1.3.Final"
  private val pathyVersion      = "0.2.1"
  private val raptureVersion    = "2.0.0-M6"
  private val refinedVersion    = "0.5.0"
  private val scalazVersion     = "7.2.4"
  private val scodecBitsVersion = "1.1.0"
  private val shapelessVersion  = "2.3.1"
  private val slcVersion        = "0.4"
  private val scalacheckVersion = "1.12.5"
  private val specsVersion      = "3.8.4-scalacheck-1.12.5"

  def foundation = Seq(
    "org.scalaz"                 %% "scalaz-core"               % scalazVersion force(),
    "org.scalaz"                 %% "scalaz-concurrent"         %     scalazVersion,
    "org.scalaz"                 %% "scalaz-iteratee"           %     scalazVersion,
    "org.scalaz.stream"          %% "scalaz-stream"             %       "0.8.3a",
    "com.github.julien-truffaut" %% "monocle-core"              %    monocleVersion,
    "io.argonaut"                %% "argonaut"                  %    argonautVersion,
    "io.argonaut"                %% "argonaut-scalaz"           %    argonautVersion,
    "org.typelevel"              %% "shapeless-scalaz"          %      slcVersion,
    "com.slamdata"               %% "matryoshka-core"           %       "0.11.1",
    "com.slamdata"               %% "pathy-core"                %     pathyVersion,
    "eu.timepit"                 %% "refined"                   %    refinedVersion,
    "com.chuusai"                %% "shapeless"                 %   shapelessVersion,
    "org.scalacheck"             %% "scalacheck"                %   scalacheckVersion    % Test force(),
    "com.github.mpilquist"       %% "simulacrum"                %        "0.7.0"         %     Test,
    "org.typelevel"              %% "discipline"                %         "0.5"          %     Test,
    "org.specs2"                 %% "specs2-core"               %      specsVersion      %     Test,
    "org.scalaz"                 %% "scalaz-scalacheck-binding" %     scalazVersion      %     Test,
    "org.typelevel"              %% "shapeless-scalacheck"      %       slcVersion       %     Test,
    "org.typelevel"              %% "scalaz-specs2"             %        "0.4.0"         %     Test
  )
  def effect = Seq(
    "org.threeten"                % "threetenbp"     %    "1.3.2"
  )
  def core = Seq(
    "com.github.tototoshi"       %% "scala-csv"      %    "1.3.1",
    "com.github.julien-truffaut" %% "monocle-macro"  % monocleVersion,
    "org.http4s"                 %% "http4s-core"    % http4sVersion,
    "com.slamdata"               %% "pathy-argonaut" %  pathyVersion
  )
  def main = Seq(
    "com.github.scopt" %% "scopt" % "3.5.0",
    "org.jboss.aesh"    % "aesh"  % "0.66.8"
  )
  def mongodb = Seq(
    "org.mongodb" % "mongodb-driver-async" %   "3.2.2",
    "io.netty"    % "netty-buffer"         % nettyVersion,
    "io.netty"    % "netty-handler"        % nettyVersion
  )
  def web = Seq(
    "ch.qos.logback"  % "logback-classic"     %      "1.1.7",
    "org.scodec"     %% "scodec-scalaz"       %     "1.3.0a",
    "org.scodec"     %% "scodec-bits"         % scodecBitsVersion,
    "org.http4s"     %% "http4s-dsl"          %   http4sVersion,
    "org.http4s"     %% "http4s-argonaut62"   %   http4sVersion,
    "org.http4s"     %% "http4s-blaze-server" %   http4sVersion,
    "org.http4s"     %% "http4s-blaze-client" %   http4sVersion    % Test,
    "com.propensive" %% "rapture-json"        %   raptureVersion   % Test,
    "com.propensive" %% "rapture-json-json4s" %   raptureVersion   % Test,
    "eu.timepit"     %% "refined-scalacheck"  %   refinedVersion   % Test
  )
}

package quasar.project

import scala.Boolean
import scala.collection.Seq

import sbt._

object Dependencies {
  private val algebraVersion      = "0.7.0"
  private val argonautVersion     = "6.2.2"
  private val catsEffectVersion   = "1.0.0"
  private val disciplineVersion   = "0.7.2"
  private val jawnVersion         = "0.13.0"
  private val jawnfs2Version      = "0.13.0"
  private val matryoshkaVersion   = "0.18.3"
  private val monocleVersion      = "1.4.0"
  private val pathyVersion        = "0.2.11"
  private val refinedVersion      = "0.8.3"
  private val scodecBitsVersion   = "1.1.2"
  private val scodecScalazVersion = "1.4.1a"
  private val scalacheckVersion   = "1.14.0"
  private val scalazVersion       = "7.2.26"
  private val scalazStreamVersion = "0.8.6a"
  private val scoptVersion        = "3.5.0"
  private val shapelessVersion    = "2.3.3"
  private val simulacrumVersion   = "0.10.0"
  private val specsVersion        = "4.3.3"
  private val spireVersion        = "0.14.1"
  private val akkaVersion         = "2.5.1"
  private val fs2Version          = "1.0.0"
  private val qdataVersion        = IO.read(file("./qdata-version")).trim
  private val tectonicVersion     = IO.read(file("./tectonic-version")).trim
  private val slf4sVersion        = "1.7.25"

  def foundation = Seq(
    "com.slamdata"               %% "slamdata-predef"           % "0.0.4",
    "org.scalaz"                 %% "scalaz-core"               % scalazVersion,
    "org.scalaz"                 %% "scalaz-concurrent"         % scalazVersion,
    "org.scalaz.stream"          %% "scalaz-stream"             % scalazStreamVersion,
    "com.codecommit"             %% "shims"                     % "1.2.1",
    "org.typelevel"              %% "cats-effect"               % catsEffectVersion,
    "co.fs2"                     %% "fs2-core"                  % fs2Version,
    "co.fs2"                     %% "fs2-io"                    % fs2Version,
    "com.github.julien-truffaut" %% "monocle-core"              % monocleVersion,
    "org.typelevel"              %% "algebra"                   % algebraVersion,
    "org.typelevel"              %% "spire"                     % spireVersion,
    "io.argonaut"                %% "argonaut"                  % argonautVersion,
    "io.argonaut"                %% "argonaut-scalaz"           % argonautVersion,
    "com.slamdata"               %% "matryoshka-core"           % matryoshkaVersion,
    "com.slamdata"               %% "pathy-core"                % pathyVersion,
    "com.slamdata"               %% "pathy-argonaut"            % pathyVersion,
    "com.slamdata"               %% "qdata-time"                % qdataVersion,
    "eu.timepit"                 %% "refined"                   % refinedVersion,
    "com.chuusai"                %% "shapeless"                 % shapelessVersion,
    "org.scalacheck"             %% "scalacheck"                % scalacheckVersion,
    "com.propensive"             %% "contextual"                % "1.0.1",
    "io.frees"                   %% "iotaz-core"                % "0.3.8",
    "com.github.mpilquist"       %% "simulacrum"                % simulacrumVersion                    % Test,
    "org.typelevel"              %% "algebra-laws"              % algebraVersion                       % Test,
    "org.typelevel"              %% "discipline"                % disciplineVersion                    % Test,
    "org.typelevel"              %% "spire-laws"                % spireVersion                         % Test,
    "org.specs2"                 %% "specs2-core"               % specsVersion                         % Test,
    "org.specs2"                 %% "specs2-scalacheck"         % specsVersion                         % Test,
    "org.specs2"                 %% "specs2-scalaz"             % specsVersion                         % Test,
    "org.scalaz"                 %% "scalaz-scalacheck-binding" % (scalazVersion + "-scalacheck-1.14") % Test,
    "com.github.alexarchambault" %% "scalacheck-shapeless_1.14" % "1.2.0"                              % Test
  )

  def api = Seq(
    "com.github.julien-truffaut" %% "monocle-macro"      % monocleVersion,
    "eu.timepit"                 %% "refined-scalaz"     % refinedVersion,
    "eu.timepit"                 %% "refined-scalacheck" % refinedVersion % Test
  )

  def common = Seq(
    "com.slamdata" %% "qdata-core" % qdataVersion,
    "com.slamdata" %% "qdata-core" % qdataVersion % "test->test" classifier "tests",
    "com.slamdata" %% "qdata-time" % qdataVersion % "test->test" classifier "tests"
  )

  def frontend = Seq(
    "com.slamdata"               %% "qdata-json"    % qdataVersion,
    "com.github.julien-truffaut" %% "monocle-macro" % monocleVersion,
    "org.typelevel"              %% "algebra-laws"  % algebraVersion % Test
  )

  def datagen = Seq(
    "com.github.scopt" %% "scopt"          % scoptVersion,
    "eu.timepit"       %% "refined-scalaz" % refinedVersion
  )

  def ejson = Seq(
    "com.slamdata" %% "qdata-core" % qdataVersion,
    "com.slamdata" %% "qdata-time" % qdataVersion,
    "com.slamdata" %% "qdata-core" % qdataVersion % "test->test" classifier "tests",
    "com.slamdata" %% "qdata-time" % qdataVersion % "test->test" classifier "tests"
  )

  def sql = Seq(
    "com.github.julien-truffaut" %% "monocle-macro" % monocleVersion,
    "org.scala-lang.modules"     %% "scala-parser-combinators" % "1.0.6"
  )

  def qsu = Seq(
    "org.slf4s" %% "slf4s-api" % slf4sVersion
  )

  def core = Seq(
    "com.github.julien-truffaut" %% "monocle-macro"             % monocleVersion,
    "com.slamdata"               %% "pathy-argonaut"            % pathyVersion
  )

  def impl = Seq(
    "org.http4s"     %% "jawn-fs2"      % jawnfs2Version,
    "org.slf4s"      %% "slf4s-api"     % slf4sVersion,
    "org.spire-math" %% "jawn-argonaut" % jawnVersion,
    "org.spire-math" %% "jawn-util"     % jawnVersion
  )

  def repl = Seq(
    "com.github.tototoshi" %% "scala-csv" % "1.3.4",
    "org.jline" % "jline" % "3.8.0"
  )

  def precog = Seq(
    "org.slf4s"            %% "slf4s-api"       % slf4sVersion,
    "org.slf4j"            %  "slf4j-log4j12"   % "1.7.16",
    "org.typelevel"        %% "spire"           % spireVersion,
    "org.scodec"           %% "scodec-scalaz"   % scodecScalazVersion,
    "org.scodec"           %% "scodec-bits"     % scodecBitsVersion,
    "org.apache.jdbm"      %  "jdbm"            % "3.0-alpha5",
    "com.typesafe.akka"    %%  "akka-actor"     % akkaVersion,
    ("org.quartz-scheduler" %  "quartz"         % "2.3.0")
      .exclude("com.zaxxer", "HikariCP-java6"), // conflict with Doobie
    "commons-io"           %  "commons-io"      % "2.5"
  )

  def blueeyes = Seq(
    "com.google.guava" % "guava" % "13.0"
  )

  def yggdrasil = Seq(
    "com.slamdata" %% "tectonic" % tectonicVersion,
    "com.slamdata" %% "tectonic-test" % tectonicVersion % "test",
    "com.codecommit" %% "smock" % "0.4.0-specs2-4.0.2" % "test"
  )

  def niflheim = Seq(
    "com.typesafe.akka"  %% "akka-actor" % akkaVersion,
    "org.typelevel"      %% "spire"      % spireVersion,
    "org.objectweb.howl" %  "howl"       % "1.0.1-1"
  )

  def it = Seq(
    "co.fs2"           %% "fs2-io"              % fs2Version          % Test,
    "io.argonaut"      %% "argonaut-monocle"    % argonautVersion     % Test,
    "eu.timepit"       %% "refined-scalacheck"  % refinedVersion      % Test,
    "io.verizon.knobs" %% "core"                % "4.0.30-scalaz-7.2" % Test
  )
}

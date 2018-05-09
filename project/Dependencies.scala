package quasar.project

import scala.Boolean
import scala.collection.Seq

import sbt._

object Dependencies {
  private val algebraVersion      = "0.7.0"
  private val argonautVersion     = "6.2"
  private val disciplineVersion   = "0.7.2"
  private val doobieVersion       = "0.4.4"
  private val jawnVersion         = "0.10.4"
  private val jacksonVersion      = "2.4.4"
  private val matryoshkaVersion   = "0.18.3"
  private val monocleVersion      = "1.4.0"
  private val pathyVersion        = "0.2.11"
  private val raptureVersion      = "2.0.0-M9"
  private val refinedVersion      = "0.8.3"
  private val scodecBitsVersion   = "1.1.2"
  private val scodecScalazVersion = "1.4.1a"
  private val http4sVersion       = "0.16.6a"
  private val scalacheckVersion   = "1.13.4"
  private val scalazVersion       = "7.2.18"
  private val scalazStreamVersion = "0.8.6a"
  private val scoptVersion        = "3.5.0"
  private val shapelessVersion    = "2.3.2"
  private val simulacrumVersion   = "0.10.0"
  // For unknown reason sbt-slamdata's specsVersion, 3.8.7,
  // leads to a ParquetRDDE failure under a full test run
  private val specsVersion        = "4.0.2"
  private val spireVersion        = "0.14.1"
  private val akkaVersion         = "2.5.1"
  private val deloreanVersion     = "1.2.42-scalaz-7.2"
  private val fs2Version          = "0.9.6"
  private val fs2ScalazVersion    = "0.2.0"

  def foundation = Seq(
    "com.slamdata"               %% "slamdata-predef"           % "0.0.4",
    "org.scalaz"                 %% "scalaz-core"               % scalazVersion,
    "org.scalaz"                 %% "scalaz-concurrent"         % scalazVersion,
    "org.scalaz.stream"          %% "scalaz-stream"             % scalazStreamVersion,
    "com.github.julien-truffaut" %% "monocle-core"              % monocleVersion,
    "org.typelevel"              %% "algebra"                   % algebraVersion,
    "org.typelevel"              %% "spire"                     % spireVersion,
    "io.argonaut"                %% "argonaut"                  % argonautVersion,
    "io.argonaut"                %% "argonaut-scalaz"           % argonautVersion,
    "com.slamdata"               %% "matryoshka-core"           % matryoshkaVersion,
    "com.slamdata"               %% "pathy-core"                % pathyVersion,
    "com.slamdata"               %% "pathy-argonaut"            % pathyVersion,
    "eu.timepit"                 %% "refined"                   % refinedVersion,
    "com.chuusai"                %% "shapeless"                 % shapelessVersion,
    "org.scalacheck"             %% "scalacheck"                % scalacheckVersion,
    "com.propensive"             %% "contextual"                % "1.0.1",
    "com.github.mpilquist"       %% "simulacrum"                % simulacrumVersion                    % Test,
    "org.typelevel"              %% "algebra-laws"              % algebraVersion                       % Test,
    "org.typelevel"              %% "discipline"                % disciplineVersion                    % Test,
    "org.typelevel"              %% "spire-laws"                % spireVersion                         % Test,
    "org.specs2"                 %% "specs2-core"               % specsVersion                         % Test,
    "org.specs2"                 %% "specs2-scalacheck"         % specsVersion                         % Test,
    "org.scalaz"                 %% "scalaz-scalacheck-binding" % (scalazVersion + "-scalacheck-1.13") % Test,
    "org.typelevel"              %% "shapeless-scalacheck"      % "0.6.1"                              % Test,
    "org.typelevel"              %% "scalaz-specs2"             % "0.5.2"                              % Test
  )

  def api = Seq(
    "com.github.julien-truffaut" %% "monocle-macro"  % monocleVersion,
    "eu.timepit"                 %% "refined-scalaz" % refinedVersion
  )

  def frontend = Seq(
    "com.github.julien-truffaut" %% "monocle-macro"            % monocleVersion,
    "org.scala-lang.modules"     %% "scala-parser-combinators" % "1.0.6",
    "org.typelevel"              %% "algebra-laws"             % algebraVersion  % Test
  )

  def ejson = Seq(
    "org.spire-math" %% "jawn-parser" % jawnVersion
  )

  def effect = Seq(
    "com.fasterxml.uuid" % "java-uuid-generator" % "3.1.4"
  )

  def datagen = Seq(
    "co.fs2"           %% "fs2-core"       % fs2Version,
    "co.fs2"           %% "fs2-io"         % fs2Version,
    "co.fs2"           %% "fs2-scalaz"     % fs2ScalazVersion,
    "com.github.scopt" %% "scopt"          % scoptVersion,
    "eu.timepit"       %% "refined-scalaz" % refinedVersion
  )

  def connector = Seq(
    "co.fs2" %% "fs2-core" % fs2Version
  )

  def core = Seq(
    "org.tpolecat"               %% "doobie-core"               % doobieVersion,
    "org.tpolecat"               %% "doobie-hikari"             % doobieVersion,
    "org.tpolecat"               %% "doobie-postgres"           % doobieVersion,
    "org.http4s"                 %% "http4s-core"               % http4sVersion,
    "com.github.julien-truffaut" %% "monocle-macro"             % monocleVersion,
    "com.github.tototoshi"       %% "scala-csv"                 % "1.3.4",
    "com.slamdata"               %% "pathy-argonaut"            % pathyVersion,
    // Removing this will not cause any compile time errors, but will cause a runtime error once
    // Quasar attempts to connect to an h2 database to use as a metastore
    "com.h2database"              % "h2"                        % "1.4.196",
    "org.tpolecat"               %% "doobie-specs2"             % doobieVersion % Test
  )

  def interface = Seq(
    "com.github.scopt" %% "scopt" % scoptVersion,
    "org.jboss.aesh"    % "aesh"  % "0.66.17"
  )

  def mongodb = {
    val nettyVersion = "4.1.21.Final"

    Seq(
      "org.mongodb" % "mongodb-driver-async" %   "3.6.3",
      // These are optional dependencies of the mongo asynchronous driver.
      // They are needed to connect to mongodb vis SSL which we do under certain configurations
      "io.netty"    % "netty-buffer"         % nettyVersion,
      "io.netty"    % "netty-handler"        % nettyVersion
    )
  }

  def marklogic = Seq(
    "com.fasterxml.jackson.core" %  "jackson-core"         % jacksonVersion,
    "com.fasterxml.jackson.core" %  "jackson-databind"     % jacksonVersion,
    "com.marklogic"              %  "marklogic-xcc"        % "8.0.5",
    "com.slamdata"               %% "xml-names-core"       % "0.0.1",
    "org.scala-lang.modules"     %% "scala-xml"            % "1.0.6",
    "eu.timepit"                 %% "refined-scalaz"       % refinedVersion,
    "eu.timepit"                 %% "refined-scalacheck"   % refinedVersion % Test,
    "com.slamdata"               %% "xml-names-scalacheck" % "0.0.1"        % Test
  )
  val couchbase = Seq(
    "com.couchbase.client" %  "java-client" % "2.3.5",
    "io.reactivex"         %% "rxscala"     % "0.26.4",
    "org.http4s"           %% "http4s-core" % http4sVersion,
    "log4j"                %  "log4j"       % "1.2.17" % Test
  )
  def web = Seq(
    "eu.timepit"     %% "refined-scalaz"      % refinedVersion,
    "org.http4s"     %% "http4s-dsl"          % http4sVersion,
    "org.http4s"     %% "http4s-argonaut"     % http4sVersion,
    "org.http4s"     %% "http4s-client"       % http4sVersion,
    "org.http4s"     %% "http4s-server"       % http4sVersion,
    "org.http4s"     %% "http4s-blaze-server" % http4sVersion,
    "org.http4s"     %% "http4s-blaze-client" % http4sVersion,
    "org.scodec"     %% "scodec-scalaz"       % scodecScalazVersion,
    "org.scodec"     %% "scodec-bits"         % scodecBitsVersion,
    "com.propensive" %% "rapture-json"        % raptureVersion     % Test,
    "com.propensive" %% "rapture-json-json4s" % raptureVersion     % Test,
    "eu.timepit"     %% "refined-scalacheck"  % refinedVersion     % Test
  )
  def precog = Seq(
    "org.slf4s"            %% "slf4s-api"       % "1.7.25",
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
    "com.google.guava" %  "guava" % "13.0"
  )
  def mimir = Seq(
    "io.verizon.delorean" %% "core" % deloreanVersion,
    "co.fs2" %% "fs2-core"   % fs2Version,
    "co.fs2" %% "fs2-scalaz" % fs2ScalazVersion
  )
  def yggdrasil = Seq(
    "io.verizon.delorean" %% "core" % deloreanVersion,
    "co.fs2" %% "fs2-core"   % fs2Version,
    "co.fs2" %% "fs2-io"     % fs2Version,
    "co.fs2" %% "fs2-scalaz" % fs2ScalazVersion,
    "com.codecommit" %% "smock" % "0.3.1-specs2-4.0.2" % "test"
  )
  def niflheim = Seq(
    "com.typesafe.akka"  %% "akka-actor" % akkaVersion,
    "org.typelevel"      %% "spire"      % spireVersion,
    "org.objectweb.howl" %  "howl"       % "1.0.1-1"
  )
  def it = Seq(
    "io.argonaut"      %% "argonaut-monocle"    % argonautVersion     % Test,
    "org.http4s"       %% "http4s-blaze-client" % http4sVersion       % Test,
    "eu.timepit"       %% "refined-scalacheck"  % refinedVersion      % Test,
    "io.verizon.knobs" %% "core"                % "4.0.30-scalaz-7.2" % Test
  )
}

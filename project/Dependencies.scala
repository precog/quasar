package quasar.project

import scala.{Option, Boolean}
import java.lang.{String, System}
import scala.collection.Seq

import sbt._, Keys._

object Dependencies {
  private val algebraVersion      = "0.7.0"
  private val argonautVersion     = "6.2"
  private val disciplineVersion   = "0.5"
  // Upgrading to doobie `0.4.1` is not trivial for a relatively minor annoyance that
  // will be addressed in `0.4.2` in which case the only thing I think needed to upgrade will be
  // changing `AnalysisSpec` to `TaskChecker`. Also, the "contrib" dependencies no longer have
  // contrib in their name, so they need to be updated. As well as `postgresql` having been changed to `postgres`
  private val doobieVersion       = "0.3.0"
  private val jawnVersion         = "0.10.4"
  private val jacksonVersion      = "2.4.4"
  private val matryoshkaVersion   = "0.18.3"
  private val monocleVersion      = "1.4.0"
  private val pathyVersion        = "0.2.11"
  private val raptureVersion      = "2.0.0-M9"
  private val refinedVersion      = "0.8.2"
  private val scodecBitsVersion   = "1.1.0"
  private val http4sVersion       = "0.15.13a"
  private val scalacheckVersion   = "1.13.4"
  private val scalazVersion       = "7.2.14"
  private val scalazStreamVersion = "0.8.6a"
  private val shapelessVersion    = "2.3.2"
  private val simulacrumVersion   = "0.10.0"
  // For unknown reason sbt-slamdata's specsVersion, 3.8.7,
  // leads to a ParquetRDDE failure under a full test run
  private val specsVersion        = "3.8.4"
  private val spireVersion        = "0.14.1"

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
    "org.scalaz"                 %% "scalaz-scalacheck-binding" % (scalazVersion + "-scalacheck-1.13") % Test,
    "org.typelevel"              %% "shapeless-scalacheck"      % "0.6"                                % Test,
    "org.typelevel"              %% "scalaz-specs2"             % "0.5.0"                              % Test
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
  def core = Seq(
    "org.tpolecat"               %% "doobie-core"               % doobieVersion,
    "org.tpolecat"               %% "doobie-contrib-hikari"     % doobieVersion,
    "org.tpolecat"               %% "doobie-contrib-postgresql" % doobieVersion,
    "org.http4s"                 %% "http4s-core"               % http4sVersion,
    "com.github.julien-truffaut" %% "monocle-macro"             % monocleVersion,
    "com.github.tototoshi"       %% "scala-csv"                 % "1.3.4",
    "com.slamdata"               %% "pathy-argonaut"            % pathyVersion,
    "org.tpolecat"               %% "doobie-contrib-specs2"     % doobieVersion % Test,
    "org.tpolecat"               %% "doobie-contrib-h2"         % doobieVersion % Test
  )
  def interface = Seq(
    "com.github.scopt" %% "scopt" % "3.5.0",
    "org.jboss.aesh"    % "aesh"  % "0.66.17",
    "com.h2database"    % "h2"    % "1.4.195"
  )

  def mongodb = {
    val nettyVersion = "4.0.42.Final" // This version is set to be the same as Spark
                                      // to avoid problems in web and it where their classpaths get merged
                                      // In any case, it should be binary compatible with version 4.0.26 that this
                                      // mongo release is expecting
    Seq(
      "org.mongodb" % "mongodb-driver-async" %   "3.3.0", // Intentionnally not upgrading to the latest 3.4.1 in order
                                                          // to make integration easier with Spark as the latest version
                                                          // depends on netty 4.1.x
      // These are optional dependencies of the mongo asynchronous driver.
      // They are needed to connect to mongodb vis SSL which we do under certain configurations
      "io.netty"    % "netty-buffer"         % nettyVersion,
      "io.netty"    % "netty-handler"        % nettyVersion
    )
  }

  def sparkcore(sparkProvided: Boolean) = Seq(
    ("org.apache.spark" %% "spark-core" % "2.1.1" % (if(sparkProvided) "provided" else "compile"))
      .exclude("aopalliance", "aopalliance")                  // It seems crazy that we need to do this,
      .exclude("javax.inject", "javax.inject")                // but it looks like Spark had some dependency conflicts
      .exclude("commons-collections", "commons-collections")  // among its transitive dependencies which means
      .exclude("commons-beanutils", "commons-beanutils-core") // we need to exclude this stuff so that we can
      .exclude("commons-logging", "commons-logging")          // create an assembly jar without conflicts
      .exclude("commons-logging", "commons-logging")          // It would seem though that things work without them...
      .exclude("com.esotericsoftware.minlog", "minlog")       // It's likely this list will need to be updated
      .exclude("org.spark-project.spark", "unused")           // anytime the Spark dependency itselft is updated
      .exclude("org.scalatest", "scalatest_2.11"),
    "org.apache.parquet"     % "parquet-format"          % "2.3.1",
    "org.apache.parquet"     % "parquet-hadoop"          % "1.9.0",
    "org.apache.spark" %% "spark-sql" % "2.1.0" % (if(sparkProvided) "provided" else "compile"),
    "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.0-M3",
    "org.http4s"             %% "http4s-core"            % http4sVersion,
    "org.http4s"             %% "http4s-blaze-client"    % http4sVersion,
    "org.elasticsearch"      %% "elasticsearch-spark-20" % "5.4.1",
    ("com.sksamuel.elastic4s" %% "elastic4s-http"         % "5.4.6")
      .exclude("commons-logging", "commons-logging"),
    "io.verizon.delorean" %% "core" % "1.2.42-scalaz-7.2",
    "com.sksamuel.elastic4s" %% "elastic4s-jackson"      % "5.4.6",
    ("com.sksamuel.elastic4s" %% "elastic4s-testkit"      % "5.4.6" % Test)
      .exclude("org.scalatest", "scalatest_2.11"),
    "org.apache.logging.log4j"              % "log4j-core"                % "2.6.2"
  )

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
    "io.reactivex"         %% "rxscala"     % "0.26.3",
    "org.http4s"           %% "http4s-core" % http4sVersion,
    "log4j"                %  "log4j"       % "1.2.17" % Test
  )
  def web = Seq(
    "org.http4s"     %% "http4s-dsl"          % http4sVersion,
    "org.http4s"     %% "http4s-argonaut"     % http4sVersion,
    "org.http4s"     %% "http4s-client"       % http4sVersion,
    "org.http4s"     %% "http4s-server"       % http4sVersion,
    "org.http4s"     %% "http4s-blaze-server" % http4sVersion,
    "org.http4s"     %% "http4s-blaze-client" % http4sVersion,
    "org.scodec"     %% "scodec-scalaz"       % "1.3.0a",
    "org.scodec"     %% "scodec-bits"         % scodecBitsVersion,
    "com.propensive" %% "rapture-json"        % raptureVersion     % Test,
    "com.propensive" %% "rapture-json-json4s" % raptureVersion     % Test,
    "eu.timepit"     %% "refined-scalacheck"  % refinedVersion     % Test
  )
  def precog = Seq(
    "org.slf4s"            %% "slf4s-api"       % "1.7.13",
    "org.slf4j"            %  "slf4j-log4j12"   % "1.7.16",
    "org.typelevel"        %% "spire"           % spireVersion,
    "org.scodec"           %% "scodec-scalaz"   % "1.3.0a",
    "org.apache.jdbm"      %  "jdbm"            % "3.0-alpha5",
    "com.typesafe.akka"    %  "akka-actor_2.11" % "2.5.1",
    "org.quartz-scheduler" %  "quartz"          % "2.3.0",
    "commons-io"           %  "commons-io"      % "2.5",
    "org.scodec"           %% "scodec-bits"     % scodecBitsVersion
  )
  def it = Seq(
    "io.argonaut"      %% "argonaut-monocle"    % argonautVersion     % Test,
    "org.http4s"       %% "http4s-blaze-client" % http4sVersion       % Test,
    "eu.timepit"       %% "refined-scalacheck"  % refinedVersion      % Test,
    "io.verizon.knobs" %% "core"                % "4.0.30-scalaz-7.2" % Test)
}

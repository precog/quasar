package quasar.project

import scala.{Option, Boolean}
import java.lang.System
import scala.collection.Seq

import sbt._, Keys._

object Dependencies {
  // Switch to `6.2-RC2` once http4s can be upgraded (see below)
  private val argonautVersion   = "6.2-M3"
  private val doobieVersion     = "0.3.0"
  // TODO: Upgrade to `0.15.2a` (or above) once we can figure out a fix for:
  // https://github.com/quasar-analytics/quasar/issues/1852
  // Although this issue will be closed by the current commit that downgrades it,
  // it's still an issue that needs to be considered for anyone attempting to upgrade
  private val http4sVersion     = "0.14.1a"
  private val jawnVersion       = "0.8.4"
  private val jacksonVersion    = "2.4.4"
  private val monocleVersion    = "1.3.2"
  private val pathyVersion      = "0.2.2"
  private val raptureVersion    = "2.0.0-M6"
  private val refinedVersion    = "0.5.0"
  private val scalazVersion     = "7.2.8"
  private val scodecBitsVersion = "1.1.0"
  private val shapelessVersion  = "2.3.1"
  private val slcVersion        = "0.4"
  private val scalacheckVersion = "1.12.5"
  private val specsVersion      = "3.8.4-scalacheck-1.12.5"

  def foundation = Seq(
    "org.scalaz"                 %% "scalaz-core"               %   scalazVersion force(),
    "org.scalaz"                 %% "scalaz-concurrent"         %   scalazVersion,
    "org.scalaz"                 %% "scalaz-iteratee"           %   scalazVersion,
    "org.scalaz.stream"          %% "scalaz-stream"             %     "0.8.3a",
    "com.github.julien-truffaut" %% "monocle-core"              %  monocleVersion,
    "io.argonaut"                %% "argonaut"                  %  argonautVersion,
    "io.argonaut"                %% "argonaut-scalaz"           %  argonautVersion,
    "org.typelevel"              %% "shapeless-scalaz"          %    slcVersion,
    "com.slamdata"               %% "matryoshka-core"           %     "0.16.4",
    "com.slamdata"               %% "pathy-core"                %   pathyVersion,
    "com.slamdata"               %% "pathy-argonaut"            %   pathyVersion    %     Test,
    "eu.timepit"                 %% "refined"                   %  refinedVersion,
    "com.chuusai"                %% "shapeless"                 % shapelessVersion,
    "org.scalacheck"             %% "scalacheck"                % scalacheckVersion % Test force(),
    "com.github.mpilquist"       %% "simulacrum"                %      "0.8.0"      %     Test,
    "org.typelevel"              %% "discipline"                %       "0.5"       %     Test,
    "org.specs2"                 %% "specs2-core"               %    specsVersion   %     Test,
    "org.scalaz"                 %% "scalaz-scalacheck-binding" %   scalazVersion   %     Test,
    "org.typelevel"              %% "shapeless-scalacheck"      %     slcVersion    %     Test,
    "org.typelevel"              %% "scalaz-specs2"             %      "0.4.0"      %     Test
  )

  def frontend = Seq(
    "com.github.julien-truffaut" %% "monocle-macro"  % monocleVersion
  )

  def ejson = Seq(
    "io.argonaut"                %% "argonaut"    % argonautVersion,
    "org.spire-math"             %% "jawn-parser" % jawnVersion
  )
  def effect = Seq(
    "com.fasterxml.uuid" % "java-uuid-generator" % "3.1.4"
  )
  def core = Seq(
    "com.github.tototoshi"       %% "scala-csv"      %    "1.3.1",
    "com.github.julien-truffaut" %% "monocle-macro"  % monocleVersion,
    "org.http4s"                 %% "http4s-core"    % http4sVersion,
    "com.slamdata"               %% "pathy-argonaut" %  pathyVersion
  )
  def interface = Seq(
    "com.github.scopt" %% "scopt" % "3.5.0",
    "org.jboss.aesh"    % "aesh"  % "0.66.8"
  )

  def mongodb = {
    val nettyVersion = "4.0.29.Final" // This version is set to be the same as Spark
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

  val postgresql = Seq(
    "org.tpolecat" %% "doobie-core"               % doobieVersion % "compile, test",
    "org.tpolecat" %% "doobie-contrib-postgresql" % doobieVersion % "compile, test"
  )

  def sparkcore(sparkProvided: Boolean) = Seq(
    ("org.apache.spark" %% "spark-core" % "2.0.1" % (if(sparkProvided) "provided" else "compile"))
      .exclude("aopalliance", "aopalliance")                  // It seems crazy that we need to do this,
      .exclude("javax.inject", "javax.inject")                // but it looks like Spark had some dependency conflicts
      .exclude("commons-collections", "commons-collections")  // among its transitive dependencies which means
      .exclude("commons-beanutils", "commons-beanutils-core") // we need to exclude this stuff so that we can
      .exclude("commons-logging", "commons-logging")          // create an assembly jar without conflicts
      .exclude("commons-logging", "commons-logging")          // It would seem though that things work without them...
      .exclude("com.esotericsoftware.minlog", "minlog")       // It's likely this list will need to be updated
      .exclude("org.spark-project.spark", "unused")           // anytime the Spark dependency itselft is updated
      .exclude("org.scalatest", "scalatest_2.11")
  )

  def marklogicValidation = Seq(
    "eu.timepit" %% "refined"     % refinedVersion,
    "org.scalaz" %% "scalaz-core" % scalazVersion force()
  )
  def marklogic = Seq(
    "com.fasterxml.jackson.core" %  "jackson-core"        % jacksonVersion,
    "com.fasterxml.jackson.core" %  "jackson-databind"    % jacksonVersion,
    "com.marklogic"              %  "marklogic-xcc"       % "8.0.5",
    "eu.timepit"                 %% "refined-scalacheck"  % refinedVersion % Test,
    "org.scala-lang.modules"     %% "scala-xml"           % "1.0.5"
  )
  val couchbase = Seq(
    "com.couchbase.client" %  "java-client" % "2.3.5",
    "io.reactivex"         %% "rxscala"     % "0.26.3",
    "org.http4s"           %% "http4s-core" % http4sVersion
  )
  def web = Seq(
    "org.scodec"     %% "scodec-scalaz"       %     "1.3.0a",
    "org.scodec"     %% "scodec-bits"         % scodecBitsVersion,
    "org.http4s"     %% "http4s-dsl"          %   http4sVersion,
    // TODO: Switch to `http4s-argonaut` once http4s can be upgraded (see above)
    "org.http4s"     %% "http4s-argonaut62"   %   http4sVersion,
    "org.http4s"     %% "http4s-blaze-server" %   http4sVersion,
    "org.http4s"     %% "http4s-blaze-client" %   http4sVersion    % Test,
    "com.propensive" %% "rapture-json"        %   raptureVersion   % Test,
    "com.propensive" %% "rapture-json-json4s" %   raptureVersion   % Test,
    "eu.timepit"     %% "refined-scalacheck"  %   refinedVersion   % Test
  )
  def it = Seq(
    "io.argonaut"      %% "argonaut-monocle"    % argonautVersion % Test,
    "org.http4s"       %% "http4s-blaze-client" % http4sVersion   % Test,
    "eu.timepit"       %% "refined-scalacheck"  % refinedVersion  % Test,
    "io.verizon.knobs" %% "core"                % "3.12.27a"      % Test)
}

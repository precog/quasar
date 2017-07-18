import github.GithubPlugin._
import quasar.project._

import java.lang.{ String, Integer }
import scala.{Boolean, List, Predef, None, Some, sys, Unit}, Predef.{any2ArrowAssoc, assert, augmentString}
import scala.collection.Seq
import scala.collection.immutable.Map

import de.heikoseeberger.sbtheader.HeaderPlugin
import de.heikoseeberger.sbtheader.license.Apache2_0
import sbt._, Aggregation.KeyValue, Keys._
import sbt.std.Transform.DummyTaskMap
import sbt.TestFrameworks.Specs2
import sbtrelease._, ReleaseStateTransformations._, Utilities._
import scoverage._
import slamdata.SbtSlamData.transferPublishAndTagResources

val BothScopes = "test->test;compile->compile"

// Exclusive execution settings
lazy val ExclusiveTests = config("exclusive") extend Test

val ExclusiveTest = Tags.Tag("exclusive-test")

def exclusiveTasks(tasks: Scoped*) =
  tasks.flatMap(inTask(_)(tags := Seq((ExclusiveTest, 1))))

lazy val buildSettings = commonBuildSettings ++ Seq(
  organization := "org.quasar-analytics",
  initialize := {
    val version = sys.props("java.specification.version")
    assert(
      Integer.parseInt(version.split("\\.")(1)) >= 8,
      "Java 8 or above required, found " + version)
  },

  ScoverageKeys.coverageHighlighting := true,

  scalacOptions ++= Seq(
    "-target:jvm-1.8",
    "-Ybackend:GenBCode"),

  // NB: -Xlint triggers issues that need to be fixed
  scalacOptions --= Seq(
    "-Xlint"),
  // NB: Some warts are disabled in specific projects. Here’s why:
  //   • AsInstanceOf   – wartremover/wartremover#266
  //   • others         – simply need to be reviewed & fixed
  wartremoverWarnings in (Compile, compile) --= Seq(
    Wart.Any,                   // - see wartremover/wartremover#263
    Wart.PublicInference,       // - creates many compile errors when enabled - needs to be enabled incrementally
    Wart.ImplicitParameter,     // - creates many compile errors when enabled - needs to be enabled incrementally
    Wart.ImplicitConversion,    // - see mpilquist/simulacrum#35
    Wart.Nothing),              // - see wartremover/wartremover#263
  // Normal tests exclude those tagged in Specs2 with 'exclusive'.
  testOptions in Test := Seq(Tests.Argument(Specs2, "exclude", "exclusive", "showtimes")),
  // Exclusive tests include only those tagged with 'exclusive'.
  testOptions in ExclusiveTests := Seq(Tests.Argument(Specs2, "include", "exclusive", "showtimes")),

  logBuffered in Test := isTravisBuild.value,

  console := { (console in Test).value }) // console alias test:console

val targetSettings = Seq(
  target := {
    import java.io.File

    val root = (baseDirectory in ThisBuild).value.getAbsolutePath
    val ours = baseDirectory.value.getAbsolutePath

    new File(root + File.separator + ".targets" + File.separator + ours.substring(root.length))
  }
)

// In Travis, the processor count is reported as 32, but only ~2 cores are
// actually available to run.
concurrentRestrictions in Global := {
  val maxTasks = 2
  if (isTravisBuild.value)
    // Recreate the default rules with the task limit hard-coded:
    Seq(Tags.limitAll(maxTasks), Tags.limit(Tags.ForkedTestGroup, 1))
  else
    (concurrentRestrictions in Global).value
}

// Tasks tagged with `ExclusiveTest` should be run exclusively.
concurrentRestrictions in Global += Tags.exclusive(ExclusiveTest)

lazy val publishSettings = commonPublishSettings ++ Seq(
  organizationName := "SlamData Inc.",
  organizationHomepage := Some(url("http://quasar-analytics.org")),
  homepage := Some(url("https://github.com/quasar-analytics/quasar")),
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/quasar-analytics/quasar"),
      "scm:git@github.com:quasar-analytics/quasar.git"
    )
  ))

lazy val assemblySettings = Seq(
  test in assembly := {},

  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.last
    case PathList("org", "apache", "hadoop", "yarn", xs @ _*) => MergeStrategy.last
    case PathList("com", "google", "common", "base", xs @ _*) => MergeStrategy.last
    case "log4j.properties"                                   => MergeStrategy.discard
    // After recent library version upgrades there seems to be a library pulling 
    // in the scala-lang scala-compiler 2.11.11 jar. It comes bundled with jansi OS libraries
    // which conflict with similar jansi libraries brought in by fusesource.jansi.jansi-1.11 
    // So the merge needed the following lines to avoid the "deduplicate: different file contents found" 
    // produced by web/assembly. Look into removing this once we move to scala v2.11.11.
    case s if s.endsWith("libjansi.jnilib")                   => MergeStrategy.last
    case s if s.endsWith("jansi.dll")                         => MergeStrategy.last
    case s if s.endsWith("libjansi.so")                       => MergeStrategy.last

    case other => (assemblyMergeStrategy in assembly).value apply other
  },

  assemblyExcludedJars in assembly := {
    val cp = (fullClasspath in assembly).value

    cp filter { af =>
      val file = af.data

      val excludeByName: Boolean = file.getName.matches("""scala-library-2\.11\.\d+\.jar""")
      val excludeByPath: Boolean = file.getPath.contains("org/scala-lang")

      excludeByName && excludeByPath
    }
  }
)

// Build and publish a project, excluding its tests.
lazy val commonSettings = buildSettings ++ publishSettings ++ assemblySettings

// Include to also publish a project's tests
lazy val publishTestsSettings = Seq(
  publishArtifact in (Test, packageBin) := true
)

lazy val githubReleaseSettings =
  githubSettings ++ Seq(
    GithubKeys.assets := Seq(assembly.value),
    GithubKeys.repoSlug := "quasar-analytics/quasar",
    GithubKeys.releaseName := "quasar " + GithubKeys.tag.value,
    releaseVersionFile := file("version.sbt"),
    releaseUseGlobalVersion := true,
    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies,
      inquireVersions,
      runTest,
      setReleaseVersion,
      commitReleaseVersion,
      pushChanges)
  )

lazy val isCIBuild               = settingKey[Boolean]("True when building in any automated environment (e.g. Travis)")
lazy val isIsolatedEnv           = settingKey[Boolean]("True if running in an isolated environment")
lazy val exclusiveTestTag        = settingKey[String]("Tag for exclusive execution tests")
lazy val sparkDependencyProvided = settingKey[Boolean]("Whether or not the spark dependency should be marked as provided. If building for use in a Spark cluster, one would set this to true otherwise setting it to false will allow you to run the assembly jar on it's own")

lazy val root = project.in(file("."))
  .settings(commonSettings)
  .settings(noPublishSettings)
  .settings(transferPublishAndTagResources)
  .settings(aggregate in assembly := false)
  .aggregate(
// NB: need to get dependencies to look like:
//         ┌ common ┐
//  ┌ frontend ┬ connector ┬─────────┬──────┐
// sql       core      marklogic  mongodb  ...
//  └──────────┼───────────┴─────────┴──────┘
//         interface

        foundation,
//     / / | | \ \
//
      ejson, js,
//       \  /
        common,    // <------------------------------------------------------
//        |    \                                                             \
    effect, frontend,                                                       precog,
//   |       |  |  \________________________________________________________  |
                                                                           blueeyes,
//                                                                            |
                                                                           niflheim,
//   |    |   |                                                               |
    sql, connector,                                                        yggdrasil,
//   |   /  | | \ \______ __________________________________________________  |
//   |  /   | |  \                                                          \ |
    core, couchbase, marklogic, mongodb, skeleton, sparkcore,   mimir,
//      \ \ | / /                                                           /
        interface,
//        /   \
       repl,  web,
//             |
              it)
  .enablePlugins(AutomateHeaderPlugin)

// common components

/** Very general utilities, ostensibly not Quasar-specific, but they just aren’t
  * in other places yet. This also contains `contrib` packages for things we’d
  * like to push to upstream libraries.
  */
lazy val foundation = project
  .settings(name := "quasar-foundation-internal")
  .settings(commonSettings)
  .settings(publishTestsSettings)
  .settings(targetSettings)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](version, ScoverageKeys.coverageEnabled, isCIBuild, isIsolatedEnv, exclusiveTestTag),
    buildInfoPackage := "quasar.build",
    exclusiveTestTag := "exclusive",
    isCIBuild := isTravisBuild.value,
    isIsolatedEnv := java.lang.Boolean.parseBoolean(java.lang.System.getProperty("isIsolatedEnv")),
    libraryDependencies ++= Dependencies.foundation)
  .enablePlugins(AutomateHeaderPlugin, BuildInfoPlugin)

/** A fixed-point implementation of the EJson spec. This should probably become
  * a standalone library.
  */
lazy val ejson = project
  .settings(name := "quasar-ejson-internal")
  .dependsOn(foundation % BothScopes)
  .settings(libraryDependencies ++= Dependencies.ejson)
  .settings(commonSettings)
  .settings(targetSettings)
  .enablePlugins(AutomateHeaderPlugin)

lazy val effect = project
  .settings(name := "quasar-effect-internal")
  .dependsOn(foundation % BothScopes)
  .settings(libraryDependencies ++= Dependencies.effect)
  .settings(commonSettings)
  .settings(targetSettings)
  .enablePlugins(AutomateHeaderPlugin)

/** Somewhat Quasar- and MongoDB-specific JavaScript implementations.
  */
lazy val js = project
  .settings(name := "quasar-js-internal")
  .dependsOn(foundation % BothScopes)
  .settings(commonSettings)
  .settings(targetSettings)
  .enablePlugins(AutomateHeaderPlugin)

/** Quasar components shared by both frontend and connector. This includes
  * things like data models, types, etc.
  */
lazy val common = project
  .settings(name := "quasar-common-internal")
  // TODO: The dependency on `js` is because `Data` encapsulates its `toJs`,
  //       which should be extracted.
  .dependsOn(foundation % BothScopes, ejson % BothScopes, js % BothScopes)
  .settings(commonSettings)
  .settings(publishTestsSettings)
  .settings(targetSettings)
  .enablePlugins(AutomateHeaderPlugin)

/** The compiler from `LogicalPlan` to `QScript` – this is the bulk of
  * transformation, type checking, optimization, etc.
  */
lazy val core = project
  .settings(name := "quasar-core-internal")
  .dependsOn(frontend % BothScopes, connector % BothScopes, sql)
  .settings(commonSettings)
  .settings(publishTestsSettings)
  .settings(targetSettings)
  .settings(
    libraryDependencies ++= Dependencies.core,
    ScoverageKeys.coverageMinimum := 79,
    ScoverageKeys.coverageFailOnMinimum := true)
  .enablePlugins(AutomateHeaderPlugin)

// frontends

/** Types and operations needed by query language implementations.
  */
lazy val frontend = project
  .settings(name := "quasar-frontend-internal")
  .dependsOn(common % BothScopes)
  .settings(commonSettings)
  .settings(publishTestsSettings)
  .settings(targetSettings)
  .settings(
    libraryDependencies ++= Dependencies.frontend,
    ScoverageKeys.coverageMinimum := 79,
    ScoverageKeys.coverageFailOnMinimum := true)
  .enablePlugins(AutomateHeaderPlugin)

/** Implementation of the SQL² query language.
  */
lazy val sql = project
  .settings(name := "quasar-sql-internal")
  .dependsOn(frontend % BothScopes)
  .settings(commonSettings)
  .settings(targetSettings)
  .enablePlugins(AutomateHeaderPlugin)

// connectors

/** Types and operations needed by connector implementations.
  */
lazy val connector = project
  .settings(name := "quasar-connector-internal")
  .dependsOn(
    common   % BothScopes,
    effect   % BothScopes,
    frontend % BothScopes,
    sql      % "test->test")
  .settings(commonSettings)
  .settings(publishTestsSettings)
  .settings(targetSettings)
  .settings(
    ScoverageKeys.coverageMinimum := 79,
    ScoverageKeys.coverageFailOnMinimum := true)
  .enablePlugins(AutomateHeaderPlugin)

/** Implementation of the Couchbase connector.
  */
lazy val couchbase = project
  .settings(name := "quasar-couchbase-internal")
  .dependsOn(connector % BothScopes)
  .settings(commonSettings)
  .settings(targetSettings)
  .settings(libraryDependencies ++= Dependencies.couchbase)
  .enablePlugins(AutomateHeaderPlugin)

/** Implementation of the MarkLogic connector.
  */
lazy val marklogic = project
  .settings(name := "quasar-marklogic-internal")
  .dependsOn(connector % BothScopes)
  .settings(commonSettings)
  .settings(targetSettings)
  .settings(resolvers += "MarkLogic" at "http://developer.marklogic.com/maven2")
  .settings(libraryDependencies ++= Dependencies.marklogic)
  .enablePlugins(AutomateHeaderPlugin)

/** Implementation of the MongoDB connector.
  */
lazy val mongodb = project
  .settings(name := "quasar-mongodb-internal")
  .dependsOn(
    connector % BothScopes,
    js        % BothScopes,
    core      % "test->compile")
  .settings(commonSettings)
  .settings(targetSettings)
  .settings(
    libraryDependencies ++= Dependencies.mongodb,
    wartremoverWarnings in (Compile, compile) --= Seq(
      Wart.AsInstanceOf,
      Wart.Equals,
      Wart.Overloading))
  .enablePlugins(AutomateHeaderPlugin)

/** A connector outline, meant to be copied and incrementally filled in while
  * implementing a new connector.
  */
lazy val skeleton = project
  .settings(name := "quasar-skeleton-internal")
  .dependsOn(connector % BothScopes)
  .settings(commonSettings)
  .settings(targetSettings)
  .enablePlugins(AutomateHeaderPlugin)

/** Implementation of the Spark connector.
  */
lazy val sparkcore = project
  .settings(name := "quasar-sparkcore-internal")
  .dependsOn(
    connector % BothScopes
    )
  .settings(commonSettings)
  .settings(targetSettings)
  .settings(githubReleaseSettings)
  .settings(assemblyJarName in assembly := "sparkcore.jar")
  .settings(parallelExecution in Test := false)
  .settings(
    sparkDependencyProvided := false,
    libraryDependencies ++= Dependencies.sparkcore(sparkDependencyProvided.value))
  .enablePlugins(AutomateHeaderPlugin)

// interfaces

/** Types and operations needed by applications that embed Quasar.
  */
lazy val interface = project
  .settings(name := "quasar-interface-internal")
  .dependsOn(
    core % BothScopes,
    couchbase,
    marklogic % BothScopes,
    mongodb,
    sparkcore,
    skeleton,
    mimir)
  .settings(commonSettings)
  .settings(targetSettings)
  .settings(libraryDependencies ++= Dependencies.interface)
  .enablePlugins(AutomateHeaderPlugin)

/** An interactive REPL application for Quasar.
  */
lazy val repl = project
  .settings(name := "quasar-repl")
  .dependsOn(interface, foundation % BothScopes)
  .settings(commonSettings)
  .settings(noPublishSettings)
  .settings(githubReleaseSettings)
  .settings(targetSettings)
  .settings(
    fork in run := true,
    connectInput in run := true,
    outputStrategy := Some(StdoutOutput))
  .enablePlugins(AutomateHeaderPlugin)

/** An HTTP interface to Quasar.
  */
lazy val web = project
  .settings(name := "quasar-web")
  .dependsOn(interface % BothScopes, core % BothScopes)
  .settings(commonSettings)
  .settings(publishTestsSettings)
  .settings(githubReleaseSettings)
  .settings(targetSettings)
  .settings(
    mainClass in Compile := Some("quasar.server.Server"),
    libraryDependencies ++= Dependencies.web)
  .enablePlugins(AutomateHeaderPlugin)

/** Integration tests that have some dependency on a running connector.
  */
lazy val it = project
  .configs(ExclusiveTests)
  .dependsOn(web % BothScopes, core % BothScopes)
  .settings(commonSettings)
  .settings(noPublishSettings)
  .settings(targetSettings)
  .settings(libraryDependencies ++= Dependencies.it)
  // Configure various test tasks to run exclusively in the `ExclusiveTests` config.
  .settings(inConfig(ExclusiveTests)(Defaults.testTasks): _*)
  .settings(inConfig(ExclusiveTests)(exclusiveTasks(test, testOnly, testQuick)): _*)
  .settings(parallelExecution in Test := false)
  .enablePlugins(AutomateHeaderPlugin)


/***** PRECOG *****/

// copied from sbt-slamdata (remove redundancy when slamdata/sbt-slamdata#23 is fixed)
val headerSettings = Seq(
  headers := Map(
     ("scala", Apache2_0("2014–2017", "SlamData Inc.")),
     ("java",  Apache2_0("2014–2017", "SlamData Inc."))),
   licenses += (("Apache 2", url("http://www.apache.org/licenses/LICENSE-2.0"))),
   checkHeaders := {
     if ((createHeaders in Compile).value.nonEmpty) sys.error("headers not all present")
  })

import precogbuild.Build._

lazy val precog = project.setup
  .settings(name := "quasar-precog-internal")
  .dependsOn(common % BothScopes)
  .withWarnings
  .deps(Dependencies.precog: _*)
  .settings(headerSettings)
  .settings(publishSettings)
  .settings(assemblySettings)
  .settings(targetSettings)
  .enablePlugins(AutomateHeaderPlugin)

lazy val blueeyes = project.setup
  .settings(name := "quasar-blueeyes-internal")
  .dependsOn(precog % BothScopes, frontend)
  .withWarnings
  .settings(libraryDependencies += "com.google.guava" %  "guava" % "13.0")
  .settings(headerSettings)
  .settings(publishSettings)
  .settings(assemblySettings)
  .settings(targetSettings)
  .enablePlugins(AutomateHeaderPlugin)

lazy val mimir = project.setup
  .settings(name := "quasar-mimir-internal")
  .dependsOn(yggdrasil % BothScopes, blueeyes, precog % BothScopes, connector)
  .scalacArgs("-Ypartial-unification")
  .withWarnings
  .settings(
    libraryDependencies ++= Seq(
      "io.verizon.delorean" %% "core" % "1.2.42-scalaz-7.2",

      "co.fs2" %% "fs2-core"   % "0.9.6",
      "co.fs2" %% "fs2-scalaz" % "0.2.0"))
  .settings(headerSettings)
  .settings(publishSettings)
  .settings(assemblySettings)
  .settings(targetSettings)
  .enablePlugins(AutomateHeaderPlugin)

lazy val niflheim = project.setup
  .settings(name := "quasar-niflheim-internal")
  .dependsOn(blueeyes % BothScopes, precog % BothScopes)
  .scalacArgs("-Ypartial-unification")
  .withWarnings
  .settings(
    libraryDependencies ++= Seq(
      "com.typesafe.akka"  %% "akka-actor" % "2.3.11",
      "org.typelevel"      %% "spire"      % "0.14.1", // TODO use spireVersion from project/Dependencies.scala
      "org.objectweb.howl" %  "howl"       % "1.0.1-1"))
  .settings(headerSettings)
  .settings(publishSettings)
  .settings(assemblySettings)
  .settings(targetSettings)
  .enablePlugins(AutomateHeaderPlugin)

lazy val yggdrasil = project.setup
  .settings(name := "quasar-yggdrasil-internal")
  .dependsOn(blueeyes % BothScopes, precog % BothScopes, niflheim % BothScopes)
  .withWarnings
  .settings(
    resolvers += "bintray-djspiewak-maven" at "https://dl.bintray.com/djspiewak/maven",

    libraryDependencies ++= Seq(
      "io.verizon.delorean" %% "core" % "1.2.42-scalaz-7.2",

      "co.fs2" %% "fs2-core"   % "0.9.6",
      "co.fs2" %% "fs2-io"     % "0.9.6",
      "co.fs2" %% "fs2-scalaz" % "0.2.0",

      "com.codecommit" %% "smock" % "0.3-specs2-3.8.4" % "test"))
  .settings(headerSettings)
  .settings(publishSettings)
  .settings(assemblySettings)
  .settings(targetSettings)
  .enablePlugins(AutomateHeaderPlugin)

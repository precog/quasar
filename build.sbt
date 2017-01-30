import github.GithubPlugin._
import quasar.project._
import quasar.project.build._

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

val BothScopes = "test->test;compile->compile"

// Exclusive execution settings
lazy val ExclusiveTests = config("exclusive") extend Test

val ExclusiveTest = Tags.Tag("exclusive-test")

def exclusiveTasks(tasks: Scoped*) =
  tasks.flatMap(inTask(_)(tags := Seq((ExclusiveTest, 1))))

lazy val checkHeaders =
  taskKey[Unit]("Fail the build if createHeaders is not up-to-date")

lazy val buildSettings = Seq(
  organization := "org.quasar-analytics",
  headers := Map(
    ("scala", Apache2_0("2014–2016", "SlamData Inc.")),
    ("java",  Apache2_0("2014–2016", "SlamData Inc."))),
  scalaOrganization := "org.typelevel",
  outputStrategy := Some(StdoutOutput),
  initialize := {
    val version = sys.props("java.specification.version")
    assert(
      Integer.parseInt(version.split("\\.")(1)) >= 8,
      "Java 8 or above required, found " + version)
  },
  autoCompilerPlugins := true,
  autoAPIMappings := true,
  resolvers ++= Seq(
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snapshots"),
    "JBoss repository" at "https://repository.jboss.org/nexus/content/repositories/",
    "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases",
    "bintray/non" at "http://dl.bintray.com/non/maven"),
  addCompilerPlugin("org.spire-math"  %% "kind-projector" % "0.9.3"),
  addCompilerPlugin("org.scalamacros" %  "paradise"       % "2.1.0" cross CrossVersion.full),

  ScoverageKeys.coverageHighlighting := true,

  // NB: These options need scalac 2.11.7 ∴ sbt > 0.13 for meta-project
  scalacOptions ++= BuildInfo.scalacOptions ++ Seq(
    "-target:jvm-1.8",
    "-Ybackend:GenBCode",
    "-Ydelambdafy:method",
    "-Ypartial-unification",
    "-Yliteral-types",
    "-Ywarn-unused-import"),
  scalacOptions in (Test, console) --= Seq(
    "-Yno-imports",
    "-Ywarn-unused-import"),
  scalacOptions in (Compile, doc) -= "-Xfatal-warnings",
  // NB: Some warts are disabled in specific projects. Here’s why:
  //   • AsInstanceOf   – puffnfresh/wartremover#266
  //   • NoNeedForMonad – puffnfresh/wartremover#268
  //   • others         – simply need to be reviewed & fixed
  wartremoverWarnings in (Compile, compile) ++= Warts.allBut(
    Wart.Any,                   // - see puffnfresh/wartremover#263
    Wart.NoNeedForMonad,        // - Causes issues compiling with scoverage
    Wart.ExplicitImplicitTypes, // - see puffnfresh/wartremover#226
    Wart.ImplicitConversion,    // - see mpilquist/simulacrum#35
    Wart.Nothing),              // - see puffnfresh/wartremover#263
  // Normal tests exclude those tagged in Specs2 with 'exclusive'.
  testOptions in Test := Seq(Tests.Argument(Specs2, "exclude", "exclusive")),
  // Exclusive tests include only those tagged with 'exclusive'.
  testOptions in ExclusiveTests := Seq(Tests.Argument(Specs2, "include", "exclusive")),

  console <<= console in Test, // console alias test:console

  licenses += (("Apache 2", url("http://www.apache.org/licenses/LICENSE-2.0"))),

  checkHeaders := {
    if ((createHeaders in Compile).value.nonEmpty)
      sys.error("headers not all present")
  })

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

lazy val publishSettings = Seq(
  organizationName := "SlamData Inc.",
  organizationHomepage := Some(url("http://quasar-analytics.org")),
  homepage := Some(url("https://github.com/quasar-analytics/quasar")),
  licenses := Seq("Apache 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
  publishTo := {
    val nexus = "https://oss.sonatype.org/"
    if (isSnapshot.value)
      Some("snapshots" at nexus + "content/repositories/snapshots")
    else
      Some("releases"  at nexus + "service/local/staging/deploy/maven2")
  },
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false },
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  releaseCrossBuild := true,
  autoAPIMappings := true,
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/quasar-analytics/quasar"),
      "scm:git@github.com:quasar-analytics/quasar.git"
    )
  ),
  developers := List(
    Developer(
      id = "slamdata",
      name = "SlamData Inc.",
      email = "contact@slamdata.com",
      url = new URL("http://slamdata.com")
    )
  )
)

lazy val assemblySettings = Seq(
  test in assembly := {},

  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.last
    case PathList("org", "apache", "hadoop", "yarn", xs @ _*) => MergeStrategy.last
    case PathList("com", "google", "common", "base", xs @ _*) => MergeStrategy.last

    case other => (assemblyMergeStrategy in assembly).value apply other
  }
)

// Build and publish a project, excluding its tests.
lazy val commonSettings = buildSettings ++ publishSettings ++ assemblySettings

// Include to also publish a project's tests
lazy val publishTestsSettings = Seq(
  publishArtifact in (Test, packageBin) := true
)

// Include to prevent publishing any artifacts for a project
lazy val noPublishSettings = Seq(
  publishTo := Some(Resolver.file("nopublish repository", file("target/nopublishrepo"))),
  publish := {},
  publishLocal := {},
  publishArtifact := false
)

lazy val githubReleaseSettings =
  githubSettings ++ Seq(
    GithubKeys.assets := Seq(assembly.value),
    GithubKeys.repoSlug := "quasar-analytics/quasar",
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
  .settings(aggregate in assembly := false)
  .aggregate(
        foundation,
//     / / | | \ \    // NB: need to get dependencies to look like:
//                    //         ┌ common ┐
      ejson, js,      //  ┌ frontend ┬ connector ┬─────────┬──────┐
//       \  /         // sql       core      marklogic  mongodb  ...
        common,       //  └──────────┼───────────┴─────────┴──────┘
//        |           //         interface
    frontend, effect,
//   |    \   |
    sql, connector, marklogicValidation,
//   |  /   | | \ \      |
    core, couchbase, marklogic, mongodb, postgresql, skeleton, sparkcore,
//      \ \ | / /
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
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](version, ScoverageKeys.coverageEnabled, isCIBuild, isIsolatedEnv, exclusiveTestTag),
    buildInfoPackage := "quasar.build",
    exclusiveTestTag := "exclusive",
    isCIBuild := isTravisBuild.value,
    isIsolatedEnv := java.lang.Boolean.parseBoolean(java.lang.System.getProperty("isIsolatedEnv")),
    libraryDependencies ++= Dependencies.foundation,
    wartremoverWarnings in (Compile, compile) -= Wart.NoNeedForMonad)
  .enablePlugins(AutomateHeaderPlugin, BuildInfoPlugin)

/** A fixed-point implementation of the EJson spec. This should probably become
  * a standalone library.
  */
lazy val ejson = project
  .settings(name := "quasar-ejson-internal")
  .dependsOn(foundation % BothScopes)
  .settings(libraryDependencies ++= Dependencies.ejson)
  .settings(commonSettings)
  .enablePlugins(AutomateHeaderPlugin)

lazy val effect = project
  .settings(name := "quasar-effect-internal")
  .dependsOn(foundation % BothScopes)
  .settings(libraryDependencies ++= Dependencies.effect)
  .settings(commonSettings)
  .settings(wartremoverWarnings in (Compile, compile) --= Seq(
    Wart.AsInstanceOf,
    Wart.NoNeedForMonad))
  .enablePlugins(AutomateHeaderPlugin)

/** Somewhat Quasar- and MongoDB-specific JavaScript implementations.
  */
lazy val js = project
  .settings(name := "quasar-js-internal")
  .dependsOn(foundation % BothScopes)
  .settings(commonSettings)
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
  .settings(
    ScoverageKeys.coverageMinimum := 79,
    ScoverageKeys.coverageFailOnMinimum := true,
    wartremoverWarnings in (Compile, compile) --= Seq(
      Wart.Equals,
      Wart.NoNeedForMonad))
  .enablePlugins(AutomateHeaderPlugin)

/** The compiler from `LogicalPlan` to `QScript` – this is the bulk of
  * transformation, type checking, optimization, etc.
  */
lazy val core = project
  .settings(name := "quasar-core-internal")
  .dependsOn(frontend % BothScopes, connector % BothScopes, sql)
  .settings(commonSettings)
  .settings(publishTestsSettings)
  .settings(
    libraryDependencies ++= Dependencies.core,
    ScoverageKeys.coverageMinimum := 79,
    ScoverageKeys.coverageFailOnMinimum := true,
    wartremoverWarnings in (Compile, compile) -= Wart.AsInstanceOf)
  .enablePlugins(AutomateHeaderPlugin)

// frontends

/** Types and operations needed by query language implementations.
  */
lazy val frontend = project
  .settings(name := "quasar-frontend-internal")
  .dependsOn(common % BothScopes)
  .settings(commonSettings)
  .settings(publishTestsSettings)
  .settings(
    libraryDependencies ++= Dependencies.frontend,
    ScoverageKeys.coverageMinimum := 79,
    ScoverageKeys.coverageFailOnMinimum := true,
    wartremoverWarnings in (Compile, compile) --= Seq(
      Wart.Equals,
      Wart.NoNeedForMonad))
  .enablePlugins(AutomateHeaderPlugin)

/** Implementation of the SQL² query language.
  */
lazy val sql = project
  .settings(name := "quasar-sql-internal")
  .dependsOn(frontend % BothScopes)
  .settings(commonSettings)
  .settings(
    wartremoverWarnings in (Compile, compile) --= Seq(
      Wart.Equals,
      Wart.NoNeedForMonad))
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
  .settings(
    ScoverageKeys.coverageMinimum := 79,
    ScoverageKeys.coverageFailOnMinimum := true,
    wartremoverWarnings in (Compile, compile) --= Seq(
      Wart.AsInstanceOf,
      Wart.NoNeedForMonad))
  .enablePlugins(AutomateHeaderPlugin)

/** Implementation of the Couchbase connector.
  */
lazy val couchbase = project
  .settings(name := "quasar-couchbase-internal")
  .dependsOn(connector % BothScopes)
  .settings(commonSettings)
  .settings(libraryDependencies ++= Dependencies.couchbase)
  .settings(wartremoverWarnings in (Compile, compile) -= Wart.AsInstanceOf)
  .enablePlugins(AutomateHeaderPlugin)

/** Implementation of the MarkLogic connector.
  */
lazy val marklogic = project
  .settings(name := "quasar-marklogic-internal")
  .dependsOn(connector % BothScopes, marklogicValidation)
  .settings(commonSettings)
  .settings(resolvers += "MarkLogic" at "http://developer.marklogic.com/maven2")
  .settings(
    libraryDependencies ++= Dependencies.marklogic,
    wartremoverWarnings in (Compile, compile) --= Seq(
      Wart.AsInstanceOf,
      Wart.NoNeedForMonad,
      Wart.Overloading))
  .enablePlugins(AutomateHeaderPlugin)

lazy val marklogicValidation = project.in(file("marklogic-validation"))
  .settings(name := "quasar-marklogic-validation-internal")
  .settings(commonSettings)
  .settings(libraryDependencies ++= Dependencies.marklogicValidation)
  // TODO: Disabled until a new release of sbt-headers with exclusion is available
  //       as we don't want our headers applied to XMLChar.java
  //.enablePlugins(AutomateHeaderPlugin)

/** Implementation of the MongoDB connector.
  */
lazy val mongodb = project
  .settings(name := "quasar-mongodb-internal")
  .dependsOn(
    connector % BothScopes,
    js        % BothScopes,
    core      % "test->compile")
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Dependencies.mongodb,
    wartremoverWarnings in (Compile, compile) --= Seq(
      Wart.AsInstanceOf,
      Wart.Equals,
      Wart.NoNeedForMonad,
      Wart.Overloading))
  .enablePlugins(AutomateHeaderPlugin)

/** Implementation of the Postgresql connector.
  */
lazy val postgresql = project
  .settings(name := "quasar-postgresql-internal")
  .dependsOn(connector % BothScopes)
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Dependencies.postgresql,
    wartremoverWarnings in (Compile, compile) -= Wart.AsInstanceOf)
  .enablePlugins(AutomateHeaderPlugin)

/** A connector outline, meant to be copied and incrementally filled in while
  * implementing a new connector.
  */
lazy val skeleton = project
  .settings(name := "quasar-skeleton-internal")
  .dependsOn(connector % BothScopes)
  .settings(commonSettings)
  .enablePlugins(AutomateHeaderPlugin)

/** Implementation of the Spark connector.
  */
lazy val sparkcore = project
  .settings(name := "quasar-sparkcore-internal")
  .dependsOn(connector % BothScopes)
  .settings(commonSettings)
  .settings(assemblyJarName in assembly := "sparkcore.jar")
  .settings(
    sparkDependencyProvided := false,
    libraryDependencies ++= Dependencies.sparkcore(sparkDependencyProvided.value),
    wartremoverWarnings in (Compile, compile) --= Seq(Wart.AsInstanceOf, Wart.NoNeedForMonad))
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
    postgresql,
    sparkcore,
    skeleton)
  .settings(commonSettings)
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
  .settings(
    fork in run := true,
    connectInput in run := true,
    outputStrategy := Some(StdoutOutput),
    wartremoverWarnings in (Compile, compile) -= Wart.AsInstanceOf)
  .enablePlugins(AutomateHeaderPlugin)

/** An HTTP interface to Quasar.
  */
lazy val web = project
  .settings(name := "quasar-web")
  .dependsOn(interface % BothScopes, core % BothScopes)
  .settings(commonSettings)
  .settings(publishTestsSettings)
  .settings(githubReleaseSettings)
  .settings(
    mainClass in Compile := Some("quasar.server.Server"),
    libraryDependencies ++= Dependencies.web,
    wartremoverWarnings in (Compile, compile) --= Seq(
      Wart.NoNeedForMonad,
      Wart.Overloading))
  .enablePlugins(AutomateHeaderPlugin)

// integration tests

/** Integration tests that have some dependency on a running connector.
  */
lazy val it = project
  .configs(ExclusiveTests)
  .dependsOn(web % BothScopes, core % BothScopes)
  .settings(commonSettings)
  .settings(noPublishSettings)
  .settings(libraryDependencies ++= Dependencies.it)
  // Configure various test tasks to run exclusively in the `ExclusiveTests` config.
  .settings(inConfig(ExclusiveTests)(Defaults.testTasks): _*)
  .settings(inConfig(ExclusiveTests)(exclusiveTasks(test, testOnly, testQuick)): _*)
  .settings(parallelExecution in Test := false)
  .enablePlugins(AutomateHeaderPlugin)

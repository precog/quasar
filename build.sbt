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
  scalaVersion := "2.11.8",
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
  addCompilerPlugin("org.spire-math"  %% "kind-projector" % "0.9.0"),
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
  wartremoverWarnings in (Compile, compile) ++= Warts.allBut(
    Wart.Any,
    Wart.AsInstanceOf,
    Wart.Equals,
    Wart.ExplicitImplicitTypes, // - see puffnfresh/wartremover#226
    Wart.ImplicitConversion,    // - see puffnfresh/wartremover#242
    Wart.IsInstanceOf,
    Wart.NoNeedForMonad,        // - see puffnfresh/wartremover#159
    Wart.Nothing,
    Wart.Overloading,
    Wart.Product,               // _ these two are highly correlated
    Wart.Serializable,          // /
    Wart.ToString),
  // Normal tests exclude those tagged in Specs2 with 'exclusive'.
  testOptions in Test := Seq(Tests.Argument(Specs2, "exclude", "exclusive")),
  // Exclusive tests include only those tagged with 'exclusive'.
  testOptions in ExclusiveTests := Seq(Tests.Argument(Specs2, "include", "exclusive")),
  // Tasks tagged with `ExclusiveTest` should be run exclusively.
  concurrentRestrictions in Global := Seq(Tags.exclusive(ExclusiveTest)),

  console <<= console in Test, // console alias test:console

  licenses += (("Apache 2", url("http://www.apache.org/licenses/LICENSE-2.0"))),

  checkHeaders := {
    if ((createHeaders in Compile).value.nonEmpty)
      sys.error("headers not all present")
  })

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

lazy val isCIBuild        = settingKey[Boolean]("True when building in any automated environment (e.g. Travis)")
lazy val isIsolatedEnv    = settingKey[Boolean]("True if running in an isolated environment")
lazy val exclusiveTestTag = settingKey[String]("Tag for exclusive execution tests")

lazy val root = project.in(file("."))
  .settings(commonSettings)
  .settings(noPublishSettings)
  .settings(aggregate in assembly := false)
  .aggregate(
        foundation,
//     / / | | \ \
//
   ejson, effect, js, // NB: need to get dependencies to look like:
//          |
         frontend,    //   frontend, connector,
//          |               /    \  /     \
           sql,       //  sql,  core,    marklogic, mongodb, ...
//          |                \    |     /
        connector,    //      interface,
//      / / | \ \
  core, marklogic, mongodb, postgresql, skeleton, sparkcore,
//      \ \ | / /
        interface,
//        /  \
      repl,   web,
//        \  /
           it)
  .enablePlugins(AutomateHeaderPlugin)

// common components

lazy val foundation = project
  .settings(name := "quasar-foundation-internal")
  .settings(commonSettings)
  .settings(publishTestsSettings)
  .settings(libraryDependencies ++= Dependencies.foundation,
    isCIBuild := sys.env contains "TRAVIS",
    isIsolatedEnv := java.lang.Boolean.parseBoolean(java.lang.System.getProperty("isIsolatedEnv")),
    exclusiveTestTag := "exclusive",
    buildInfoKeys := Seq[BuildInfoKey](version, ScoverageKeys.coverageEnabled, isCIBuild, isIsolatedEnv, exclusiveTestTag),
    buildInfoPackage := "quasar.build")
  .enablePlugins(AutomateHeaderPlugin, BuildInfoPlugin)

lazy val ejson = project
  .settings(name := "quasar-ejson-internal")
  .dependsOn(foundation % BothScopes)
  .settings(commonSettings)
  .enablePlugins(AutomateHeaderPlugin)

lazy val effect = project
  .settings(name := "quasar-effect-internal")
  .dependsOn(foundation % BothScopes)
  .settings(commonSettings)
  .enablePlugins(AutomateHeaderPlugin)

lazy val js = project
  .settings(name := "quasar-js-internal")
  .dependsOn(foundation % BothScopes)
  .settings(commonSettings)
  .enablePlugins(AutomateHeaderPlugin)

lazy val core = project
  .settings(name := "quasar-core-internal")
  .dependsOn(
    frontend % BothScopes,
    connector % BothScopes)
  .settings(commonSettings)
  .settings(publishTestsSettings)
  .settings(
    libraryDependencies ++= Dependencies.core,
    ScoverageKeys.coverageMinimum := 79,
    ScoverageKeys.coverageFailOnMinimum := true)
  .enablePlugins(AutomateHeaderPlugin)

// frontends

// TODO: This area is still tangled. It contains things that should be in `sql`,
//       things that should be in `core`, and probably other things that should
//       be elsewhere.
lazy val frontend = project
  .settings(name := "quasar-frontend-internal")
  .dependsOn(foundation % BothScopes, ejson % BothScopes, js % BothScopes)
  .settings(commonSettings)
  .settings(publishTestsSettings)
  .settings(
    libraryDependencies ++= Dependencies.core,
    ScoverageKeys.coverageMinimum := 79,
    ScoverageKeys.coverageFailOnMinimum := true)
  .enablePlugins(AutomateHeaderPlugin)

lazy val sql = project
  .settings(name := "quasar-sql-internal")
  .dependsOn(frontend % BothScopes)
  .settings(commonSettings)
  .settings(libraryDependencies ++= Dependencies.core)
  .enablePlugins(AutomateHeaderPlugin)

// connectors

lazy val connector = project
  .settings(name := "quasar-connector-internal")
  .dependsOn(
    ejson % BothScopes,
    effect % BothScopes,
    js % BothScopes,
    frontend % BothScopes,
    sql % BothScopes)
  .settings(commonSettings)
  .settings(publishTestsSettings)
  .settings(
    libraryDependencies ++= Dependencies.core,
    ScoverageKeys.coverageMinimum := 79,
    ScoverageKeys.coverageFailOnMinimum := true)
  .enablePlugins(AutomateHeaderPlugin)


lazy val marklogic = project
  .settings(name := "quasar-marklogic-internal")
  .dependsOn(connector % BothScopes, marklogicValidation)
  .settings(commonSettings)
  .settings(resolvers += "MarkLogic" at "http://developer.marklogic.com/maven2")
  .settings(libraryDependencies ++= Dependencies.marklogic)
  .enablePlugins(AutomateHeaderPlugin)

lazy val marklogicValidation = project.in(file("marklogic-validation"))
  .settings(name := "quasar-marklogic-validation-internal")
  .settings(commonSettings)
  .settings(libraryDependencies ++= Dependencies.marklogicValidation)
  // TODO: Disabled until a new release of sbt-headers with exclusion is available
  //       as we don't want our headers applied to XMLChar.java
  //.enablePlugins(AutomateHeaderPlugin)

lazy val mongodb = project
  .settings(name := "quasar-mongodb-internal")
  .dependsOn(connector % BothScopes)
  .settings(commonSettings)
  .settings(libraryDependencies ++= Dependencies.mongodb)
  .enablePlugins(AutomateHeaderPlugin)

lazy val postgresql = project
  .settings(name := "quasar-postgresql-internal")
  .dependsOn(connector % BothScopes)
  .settings(commonSettings)
  .settings(libraryDependencies ++= Dependencies.postgresql)
  .enablePlugins(AutomateHeaderPlugin)

lazy val skeleton = project
  .settings(name := "quasar-skeleton-internal")
  .dependsOn(connector % BothScopes)
  .settings(commonSettings)
  .enablePlugins(AutomateHeaderPlugin)

lazy val sparkcore = project
  .settings(name := "quasar-sparkcore-internal")
  .dependsOn(connector % BothScopes)
  .settings(commonSettings)
  .settings(libraryDependencies ++= Dependencies.sparkcore)
  .enablePlugins(AutomateHeaderPlugin)

// interfaces

lazy val interface = project
  .settings(name := "quasar-interface-internal")
  .dependsOn(
    core % BothScopes,
    marklogic,
    mongodb,
    postgresql,
    sparkcore,
    skeleton)
  .settings(commonSettings)
  .settings(libraryDependencies ++= Dependencies.interface)
  .enablePlugins(AutomateHeaderPlugin)

lazy val repl = project
  .settings(name := "quasar-repl")
  .dependsOn(interface, foundation % BothScopes)
  .settings(commonSettings)
  .settings(noPublishSettings)
  .settings(githubReleaseSettings)
  .settings(
    fork in run := true,
    connectInput in run := true,
    outputStrategy := Some(StdoutOutput))
  .enablePlugins(AutomateHeaderPlugin)

lazy val web = project
  .settings(name := "quasar-web")
  .dependsOn(interface, core % BothScopes)
  .settings(commonSettings)
  .settings(publishTestsSettings)
  .settings(githubReleaseSettings)
  .settings(
    mainClass in Compile := Some("quasar.server.Server"),
    libraryDependencies ++= Dependencies.web)
  .enablePlugins(AutomateHeaderPlugin)

// integration tests

lazy val it = project
  .configs(ExclusiveTests)
  .dependsOn(web, core % BothScopes)
  .settings(commonSettings)
  .settings(noPublishSettings)
  .settings(libraryDependencies ++= Dependencies.web)
  // Configure various test tasks to run exclusively in the `ExclusiveTests` config.
  .settings(inConfig(ExclusiveTests)(Defaults.testTasks): _*)
  .settings(inConfig(ExclusiveTests)(exclusiveTasks(test, testOnly, testQuick)): _*)
  .settings(parallelExecution in Test := false)
  .enablePlugins(AutomateHeaderPlugin)

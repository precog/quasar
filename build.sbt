import github.GithubPlugin._
import quasar.project._
import quasar.project.build._

import java.lang.Integer
import scala.{Predef, Some, sys, Unit}, Predef.{assert, augmentString}
import scala.collection.Seq
import scala.collection.immutable.Map

import de.heikoseeberger.sbtheader.HeaderPlugin
import de.heikoseeberger.sbtheader.license.Apache2_0
import sbt._, Aggregation.KeyValue, Keys._
import sbt.std.Transform.DummyTaskMap
import sbtrelease._, ReleaseStateTransformations._, Utilities._
import scoverage._

// Exclusive execution settings
lazy val ExclusiveTests = config("exclusive") extend Test

val ExclusiveTest = Tags.Tag("exclusive-test")

def exclusiveTasks(tasks: Scoped*) =
  tasks.flatMap(inTask(_)(tags := Seq((ExclusiveTest, 1))))

lazy val checkHeaders =
  taskKey[Unit]("Fail the build if createHeaders is not up-to-date")

lazy val commonSettings = Seq(
  headers := Map(
    ("scala", Apache2_0("2014–2016", "SlamData Inc.")),
    ("java",  Apache2_0("2014–2016", "SlamData Inc."))),
  scalaVersion := "2.11.8",
  outputStrategy := Some(StdoutOutput),
  initialize := {
    val version = sys.props("java.specification.version")
    assert(
      Integer.parseInt(version.split("\\.")(1)) >= 8,
      "Java 8 or above required, found " + version)
  },
  autoCompilerPlugins := true,
  autoAPIMappings := true,
  exportJars := true,
  resolvers ++= Seq(
    Resolver.sonatypeRepo("releases"),
    Resolver.sonatypeRepo("snapshots"),
    "JBoss repository" at "https://repository.jboss.org/nexus/content/repositories/",
    "Scalaz Bintray Repo" at "http://dl.bintray.com/scalaz/releases",
    "bintray/non" at "http://dl.bintray.com/non/maven"),
  addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.7.1"),
  addCompilerPlugin("org.scalamacros" % "paradise"       % "2.1.0" cross CrossVersion.full),

  ScoverageKeys.coverageHighlighting := true,

  // NB: These options need scalac 2.11.7 ∴ sbt > 0.13 for meta-project
  scalacOptions ++= BuildInfo.scalacOptions ++ Seq(
    "-target:jvm-1.8",
    // Try again once the new backend is more stable. Specifically, it would appear the Op class in Zip
    // causes problems when recompiling code in sbt without running `clean` in between.
    //"-Ybackend:GenBCode",
    "-Ydelambdafy:method",
    "-Ywarn-unused-import"),
  scalacOptions in (Test, console) --= Seq(
    "-Yno-imports",
    "-Ywarn-unused-import"),
  wartremoverErrors in (Compile, compile) ++= warts,
  // Normal tests exclude those tagged in Specs2 with 'exclusive'.
  testOptions in Test := Seq(Tests.Argument("exclude", "exclusive")),
  // Exclusive tests include only those tagged with 'exclusive'.
  testOptions in ExclusiveTests := Seq(Tests.Argument("include", "exclusive")),
  // Tasks tagged with `ExclusiveTest` should be run exclusively.
  concurrentRestrictions in Global := Seq(Tags.exclusive(ExclusiveTest)),

  console <<= console in Test, // console alias test:console

  licenses += (("Apache 2", url("http://www.apache.org/licenses/LICENSE-2.0"))),

  checkHeaders := {
    if ((createHeaders in Compile).value.nonEmpty)
      sys.error("headers not all present")
  })

// Using a Seq of desired warts instead of Warts.allBut due to an incremental compilation issue.
// https://github.com/puffnfresh/wartremover/issues/202
// omissions:
//   Wart.Any
//   Wart.AsInstanceOf
//   Wart.ExplicitImplicitTypes - see mpilquist/simulacrum#35
//   Wart.IsInstanceOf
//   Wart.NoNeedForMonad        - see puffnfresh/wartremover#159
//   Wart.Nothing
//   Wart.Product               _ these two are highly correlated
//   Wart.Serializable          /
//   Wart.Throw
//   Wart.ToString
val warts = Seq(
  Wart.Any2StringAdd,
  Wart.DefaultArguments,
  Wart.EitherProjectionPartial,
  Wart.Enumeration,
  Wart.FinalCaseClass,
  Wart.JavaConversions,
  Wart.ListOps,
  Wart.MutableDataStructures,
  Wart.NonUnitStatements,
  Wart.Null,
  Wart.Option2Iterable,
  Wart.OptionPartial,
  Wart.Return,
  Wart.TryPartial,
  Wart.Var)

lazy val oneJarSettings =
  com.github.retronym.SbtOneJar.oneJarSettings ++
    commonSettings ++
    githubSettings ++
    releaseSettings ++
    Seq(
      GithubKeys.assets := { Seq(oneJar.value) },
      GithubKeys.repoSlug := "quasar-analytics/quasar",

      ReleaseKeys.versionFile := file("version.sbt"),
      ReleaseKeys.useGlobalVersion := true,
      ReleaseKeys.releaseProcess := Seq[ReleaseStep](
        checkSnapshotDependencies,
        inquireVersions,
        runTest,
        setReleaseVersion,
        commitReleaseVersion,
        pushChanges))

lazy val root = project.in(file("."))
  .settings(commonSettings: _*)
  .aggregate(core, tests, web, it)
  .enablePlugins(AutomateHeaderPlugin)

lazy val core = project
  .settings(oneJarSettings: _*)
  .enablePlugins(AutomateHeaderPlugin, BuildInfoPlugin)

lazy val scalacheck = project
  .in(file("core-scalacheck"))
  .dependsOn(core)
  .settings(oneJarSettings: _*)  // HACK
  .settings(
    libraryDependencies ++= Dependencies.scalacheck)
  .enablePlugins(AutomateHeaderPlugin)

lazy val testUtils = project
  .in(file("core-test-utils"))
  .dependsOn(core, scalacheck)
  .settings(oneJarSettings: _*)  // HACK
  .enablePlugins(AutomateHeaderPlugin)

lazy val tests = project
  .in(file("core-tests"))
  .dependsOn(core, scalacheck % "test", testUtils % "test")
  .settings(oneJarSettings: _*)  // HACK

lazy val web = project
  .dependsOn(
    core, scalacheck % "test", testUtils % "test")
  .settings(oneJarSettings: _*)
  .settings(
    mainClass in Compile := Some("quasar.server.Server"),
    libraryDependencies ++= Dependencies.web)
  .enablePlugins(AutomateHeaderPlugin)

lazy val it = project
  .configs(ExclusiveTests)
  .dependsOn(core, testUtils % "test")
  .settings(commonSettings: _*)
  // Configure various test tasks to run exclusively in the `ExclusiveTests` config.
  .settings(inConfig(ExclusiveTests)(Defaults.testTasks): _*)
  .settings(inConfig(ExclusiveTests)(exclusiveTasks(test, testOnly, testQuick)): _*)
  .enablePlugins(AutomateHeaderPlugin)

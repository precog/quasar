import github.GithubPlugin._
import quasar.project._
import quasar.project.build._

import java.lang.Integer
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
  addCompilerPlugin("org.spire-math" %% "kind-projector"   % "0.8.0"),
  addCompilerPlugin("org.scalamacros" % "paradise"         % "2.1.0" cross CrossVersion.full),
  addCompilerPlugin("com.milessabin"  % "si2712fix-plugin" % "1.2.0" cross CrossVersion.full),

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

// Build and publish a project, excluding its tests.
lazy val commonSettings = buildSettings ++ publishSettings

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

lazy val oneJarSettings =
  com.github.retronym.SbtOneJar.oneJarSettings ++
  githubSettings ++
  Seq(
    GithubKeys.assets := { Seq(oneJar.value) },
    GithubKeys.repoSlug := "quasar-analytics/quasar",

    releaseVersionFile := file("version.sbt"),
    releaseUseGlobalVersion := true,
    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies,
      inquireVersions,
      runTest,
      setReleaseVersion,
      commitReleaseVersion,
      pushChanges))

lazy val isCIBuild = settingKey[Boolean]("True when building in any automated environment (e.g. Travis)")

lazy val root = project.in(file("."))
  .settings(commonSettings)
  .settings(noPublishSettings)
  .aggregate(
        foundation,
//     / / | | \ \
//
   ejson, effect, js,
//          |
          core,
//      / / | \ \
  mongodb, skeleton, postgresql, marklogic, // sparkcore
//      \ \ | / /
          main,
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
    buildInfoKeys := Seq[BuildInfoKey](version, ScoverageKeys.coverageEnabled, isCIBuild),
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
  .settings(libraryDependencies ++= Dependencies.effect)
  .enablePlugins(AutomateHeaderPlugin)

lazy val js = project
  .settings(name := "quasar-js-internal")
  .dependsOn(foundation % BothScopes)
  .settings(commonSettings)
  .enablePlugins(AutomateHeaderPlugin)

lazy val core = project
  .settings(name := "quasar-core-internal")
  .dependsOn(ejson % BothScopes, effect % BothScopes, js % BothScopes)
  .settings(commonSettings)
  .settings(publishTestsSettings)
  .settings(
    libraryDependencies ++= Dependencies.core,
    ScoverageKeys.coverageMinimum := 79,
    ScoverageKeys.coverageFailOnMinimum := true)
  .enablePlugins(AutomateHeaderPlugin)

lazy val main = project
  .settings(name := "quasar-main-internal")
  .dependsOn(
    mongodb    % BothScopes,
    skeleton   % BothScopes,
//  sparkcore  % BothScopes,
    postgresql % BothScopes)
  .settings(commonSettings)
  .settings(libraryDependencies ++= Dependencies.main)
  .enablePlugins(AutomateHeaderPlugin)

// filesystems (backends)

lazy val mongodb = project
  .settings(name := "quasar-mongodb-internal")
  .dependsOn(core % BothScopes)
  .settings(commonSettings)
  .settings(libraryDependencies ++= Dependencies.mongodb)
  .enablePlugins(AutomateHeaderPlugin)

lazy val skeleton = project
  .settings(name := "quasar-skeleton-internal")
  .dependsOn(core % BothScopes)
  .settings(commonSettings)
  .enablePlugins(AutomateHeaderPlugin)

lazy val marklogic = project
  .settings(name := "quasar-marklogic-internal")
  .dependsOn(core % BothScopes)
  .settings(commonSettings)
  .settings(libraryDependencies ++= Seq(
    "com.marklogic"  %  "java-client-api"   % "3.0.5",
    "org.http4s"     %% "jawn-streamz"      % "0.8.1",
    "org.spire-math" %% "jawn-argonaut"     % "0.8.4"))
  .enablePlugins(AutomateHeaderPlugin)

lazy val postgresql = project
  .settings(name := "quasar-postgresql-internal")
  .dependsOn(core % BothScopes)
  .settings(commonSettings)
  .enablePlugins(AutomateHeaderPlugin)

/* FIXME: Disabled because it breaks the Travis build
lazy val sparkcore = project
  .settings(name := "quasar-sparkcore-internal")
  .dependsOn(core % BothScopes)
  .settings(commonSettings)
  .settings(libraryDependencies +=
    "org.apache.spark" % "spark-core_2.11" % "1.6.2")
  .enablePlugins(AutomateHeaderPlugin)
*/


// frontends

// TODO: Get SQL here

// interfaces

lazy val repl = project
  .settings(name := "quasar-repl")
  .dependsOn(main % BothScopes)
  .settings(commonSettings)
  .settings(noPublishSettings)
  .settings(oneJarSettings)
  .settings(
    fork in run := true,
    connectInput in run := true,
    outputStrategy := Some(StdoutOutput))
  .enablePlugins(AutomateHeaderPlugin)

lazy val web = project
  .settings(name := "quasar-web")
  .dependsOn(main % BothScopes)
  .settings(commonSettings)
  .settings(publishTestsSettings)
  .settings(oneJarSettings)
  .settings(
    mainClass in Compile := Some("quasar.server.Server"),
    libraryDependencies ++= Dependencies.web)
  .enablePlugins(AutomateHeaderPlugin)

// integration tests

lazy val it = project
  .configs(ExclusiveTests)
  .dependsOn(web % BothScopes)
  .settings(commonSettings)
  .settings(noPublishSettings)
  // Configure various test tasks to run exclusively in the `ExclusiveTests` config.
  .settings(inConfig(ExclusiveTests)(Defaults.testTasks): _*)
  .settings(inConfig(ExclusiveTests)(exclusiveTasks(test, testOnly, testQuick)): _*)
  .enablePlugins(AutomateHeaderPlugin)

import scala.Predef._
import quasar.project._

import java.lang.{Integer, String, Throwable}
import scala.{Boolean, List, Predef, None, Some, StringContext, sys, Unit}, Predef.{any2ArrowAssoc, assert, augmentString}
import scala.collection.Seq
import scala.collection.immutable.Map
import scala.sys.process._

import sbt._, Keys._
import sbt.std.Transform.DummyTaskMap
import sbt.TestFrameworks.Specs2
import sbtrelease._, ReleaseStateTransformations._, Utilities._
import slamdata.SbtSlamData.transferPublishAndTagResources

val BothScopes = "test->test;compile->compile"

// Exclusive execution settings
lazy val ExclusiveTests = config("exclusive") extend Test

val ExclusiveTest = Tags.Tag("exclusive-test")

def exclusiveTasks(tasks: Scoped*) =
  tasks.flatMap(inTask(_)(tags := Seq((ExclusiveTest, 1))))

lazy val buildSettings = commonBuildSettings ++ Seq(
  organization := "org.quasar-analytics",
  scalaOrganization := "org.scala-lang",
  scalacOptions --= Seq(
    "-Yliteral-types",
    "-Xstrict-patmat-analysis",
    "-Yinduction-heuristics",
    "-Ykind-polymorphism",
    "-Ybackend:GenBCode"
  ),
  initialize := {
    val version = sys.props("java.specification.version")
    assert(
      Integer.parseInt(version.split("\\.")(1)) >= 8,
      "Java 8 or above required, found " + version)
  },

  scalacOptions += "-target:jvm-1.8",

  // NB: -Xlint triggers issues that need to be fixed
  scalacOptions --= Seq("-Xlint"),
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

lazy val backendRewrittenRunSettings = Seq(
  run := {
    val delegate = streams.value.log
    val args = complete.DefaultParsers.spaceDelimited("<arg>").parsed

    delegate.info("Computing classpaths of dependent backends...")

    val parentCp = (fullClasspath in connector in Compile).value.files
    val backends = isolatedBackends.value map {
      case (name, childCp) =>
        val classpathStr =
          createBackendEntry(childCp, parentCp).map(_.getAbsolutePath).mkString(",")

        "--backend:" + name + "=" + classpathStr
    }

    val main = (mainClass in Compile).value.getOrElse(sys.error("unspecified main class; huzzah huzzah huzzah"))
    val r = runner.value

    val prefix = s"Running ${main}"

    val filtered = new Logger {
      def log(level: Level.Value, _message: => String): Unit = {
        lazy val message = _message

        if (level == Level.Info && message.startsWith(prefix))
          delegate.info(prefix + "...")
        else
          delegate.log(level, message)
      }
      def success(message: => String): Unit = delegate.success(message)
      def trace(t: => Throwable): Unit = delegate.trace(t)
    }

    r.run(main, (fullClasspath in Compile).value.files, args ++ backends, filtered)
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

version in ThisBuild := {
  val currentVersion = (version in ThisBuild).value
  if (!isTravisBuild.value)
    currentVersion + "-" + "git rev-parse HEAD".!!.substring(0, 7)
  else
    currentVersion
}

useGpg in Global := {
  val oldValue = (useGpg in Global).value
  !isTravisBuild.value || oldValue
}

lazy val publishSettings = commonPublishSettings ++ Seq(
  performMavenCentralSync := false,   // publishes quasar to bintray only, skipping sonatype and maven central
  organizationName := "SlamData Inc.",
  organizationHomepage := Some(url("http://quasar-analytics.org")),
  homepage := Some(url("https://github.com/quasar-analytics/quasar")),
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/quasar-analytics/quasar"),
      "scm:git@github.com:quasar-analytics/quasar.git"
    )
  ),
  bintrayCredentialsFile := {
    val oldValue = bintrayCredentialsFile.value
    if (!isTravisBuild.value)
      Path.userHome / ".bintray" / ".credentials"
    else
      oldValue
  })

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
    cp filter { attributedFile =>
      val file = attributedFile.data

      val excludeByName: Boolean = file.getName.matches("""scala-library-2\.12\.\d+\.jar""")
      val excludeByPath: Boolean = file.getPath.contains("org/typelevel")

      excludeByName && excludeByPath
    }
  }
)

// Build and publish a project, excluding its tests.
lazy val commonSettings = buildSettings ++ publishSettings ++ assemblySettings

// not doing this causes NoSuchMethodErrors when using coursier
lazy val excludeTypelevelScalaLibrary =
  Seq(excludeDependencies += "org.typelevel" % "scala-library")

// Include to also publish a project's tests
lazy val publishTestsSettings = Seq(
  publishArtifact in (Test, packageBin) := true
)

def isolatedBackendSettings(classnames: String*) = Seq(
  isolatedBackends in Global ++=
    classnames.map(_ -> (fullClasspath in Compile).value.files),

  packageOptions in (Compile, packageBin) +=
    Package.ManifestAttributes("Backend-Module" -> classnames.mkString(" ")))

lazy val isCIBuild               = settingKey[Boolean]("True when building in any automated environment (e.g. Travis)")
lazy val isIsolatedEnv           = settingKey[Boolean]("True if running in an isolated environment")
lazy val exclusiveTestTag        = settingKey[String]("Tag for exclusive execution tests")

lazy val isolatedBackends =
  taskKey[Seq[(String, Seq[File])]]("Global-only setting which contains all of the classpath-isolated backends")

isolatedBackends in Global := Seq()

lazy val sideEffectTestFSConfig = taskKey[Unit]("Rewrite the JVM environment to contain the filesystem classpath information for integration tests")

def createBackendEntry(childPath: Seq[File], parentPath: Seq[File]): Seq[File] =
  (childPath.toSet -- parentPath.toSet).toSeq

lazy val root = project.in(file("."))
  .settings(commonSettings)
  .settings(noPublishSettings)
  .settings(transferPublishAndTagResources)
  .settings(aggregate in assembly := false)
  .settings(excludeTypelevelScalaLibrary)
  .aggregate(

       foundation, //___
//    /    \      \     \
    api, effect, ejson, js, //______
//       /     \   /                \
               common,
//     /      /      \                \
        frontend,    precog,
//    |/    /    \       |             |
     fs, sql, datagen, blueeyes,
//    |   |              |             |
// ___|___|              |             |
// |  |                  |             |
     qscript,         niflheim,
// |  |                  |             |
     qsu,
// |     \               |             |
         connector,   yggdrasil,
// |     /   |   \______|______________|_________
//  \   /    |         /     \         \         \
    core, skeleton, mimir, marklogic, mongodb, couchbase,
//      \     |    /          |          |         |
          interface,
//          /  \              |          |         |
         repl, web,
//              |             |          |         |
                it,
//   ___________|_____________/          |         |
//  /           |      __________________/         |
//  |          /|\    /          __________________/
//  |         / | \  /          /
//  |        /  |  \/__________/
//  |       /   |  /    \     /
  marklogicIt, mongoIt, couchbaseIt
//
// NB: the *It projects are temporary until we polyrepo
  ).enablePlugins(AutomateHeaderPlugin)

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
    buildInfoKeys := Seq[BuildInfoKey](version, isCIBuild, isIsolatedEnv, exclusiveTestTag),
    buildInfoPackage := "quasar.build",
    exclusiveTestTag := "exclusive",
    isCIBuild := isTravisBuild.value,
    isIsolatedEnv := java.lang.Boolean.parseBoolean(java.lang.System.getProperty("isIsolatedEnv")),
    libraryDependencies ++= Dependencies.foundation)
  .settings(excludeTypelevelScalaLibrary)
  .enablePlugins(AutomateHeaderPlugin, BuildInfoPlugin)

/** Types and interfaces describing Quasar's functionality. */
lazy val api = project
  .settings(name := "quasar-api-internal")
  .dependsOn(foundation % BothScopes)
  .settings(libraryDependencies ++= Dependencies.api)
  .settings(commonSettings)
  .settings(targetSettings)
  .settings(excludeTypelevelScalaLibrary)
  .enablePlugins(AutomateHeaderPlugin)

/** A fixed-point implementation of the EJson spec. This should probably become
  * a standalone library.
  */
lazy val ejson = project
  .settings(name := "quasar-ejson-internal")
  .dependsOn(foundation % BothScopes)
  .settings(libraryDependencies ++= Dependencies.ejson)
  .settings(commonSettings)
  .settings(targetSettings)
  .settings(excludeTypelevelScalaLibrary)
  .enablePlugins(AutomateHeaderPlugin)

lazy val effect = project
  .settings(name := "quasar-effect-internal")
  .dependsOn(foundation % BothScopes)
  .settings(libraryDependencies ++= Dependencies.effect)
  .settings(commonSettings)
  .settings(targetSettings)
  .settings(excludeTypelevelScalaLibrary)
  .enablePlugins(AutomateHeaderPlugin)

/** Somewhat Quasar- and MongoDB-specific JavaScript implementations.
  */
lazy val js = project
  .settings(name := "quasar-js-internal")
  .dependsOn(foundation % BothScopes)
  .settings(commonSettings)
  .settings(targetSettings)
  .settings(excludeTypelevelScalaLibrary)
  .enablePlugins(AutomateHeaderPlugin)

/** Quasar components shared by both frontend and connector. This includes
  * things like data models, types, etc.
  */
lazy val common = project
  .settings(name := "quasar-common-internal")
  // TODO: The dependency on `js` is because `Data` encapsulates its `toJs`,
  //       which should be extracted.
  .dependsOn(
    ejson % BothScopes,
    js % BothScopes)
  .settings(commonSettings)
  .settings(publishTestsSettings)
  .settings(targetSettings)
  .settings(excludeTypelevelScalaLibrary)
  .enablePlugins(AutomateHeaderPlugin)

/** Types and operations needed by query language implementations.
  */
lazy val frontend = project
  .settings(name := "quasar-frontend-internal")
  .dependsOn(common % BothScopes)
  .settings(commonSettings)
  .settings(publishTestsSettings)
  .settings(targetSettings)
  .settings(
    libraryDependencies ++= Dependencies.frontend)
  .settings(excludeTypelevelScalaLibrary)
  .enablePlugins(AutomateHeaderPlugin)

lazy val datagen = project
  .settings(name := "quasar-datagen")
  .dependsOn(frontend % BothScopes)
  .settings(commonSettings)
  .settings(targetSettings)
  .settings(excludeTypelevelScalaLibrary)
  .settings(
    mainClass in Compile := Some("quasar.datagen.Main"),
    libraryDependencies ++= Dependencies.datagen)
  .enablePlugins(AutomateHeaderPlugin)

/** Implementation of the SQL² query language.
  */
lazy val sql = project
  .settings(name := "quasar-sql-internal")
  .dependsOn(frontend % BothScopes)
  .settings(commonSettings)
  .settings(targetSettings)
  .settings(excludeTypelevelScalaLibrary)
  .enablePlugins(AutomateHeaderPlugin)

lazy val fs = project
  .settings(name := "quasar-fs-internal")
  .dependsOn(
    effect,
    frontend % BothScopes)
  .settings(commonSettings)
  .settings(targetSettings)
  .settings(publishTestsSettings)
  .settings(excludeTypelevelScalaLibrary)
  .enablePlugins(AutomateHeaderPlugin)

lazy val qscript = project
  .settings(name := "quasar-qscript-internal")
  .dependsOn(
    sql % "test->test",
    fs)
  .settings(commonSettings)
  .settings(targetSettings)
  .settings(excludeTypelevelScalaLibrary)
  .enablePlugins(AutomateHeaderPlugin)

lazy val qsu = project
  .settings(name := "quasar-qsu-internal")
  .dependsOn(qscript % BothScopes)
  .settings(commonSettings)
  .settings(targetSettings)
  .settings(excludeTypelevelScalaLibrary)
  .enablePlugins(AutomateHeaderPlugin)

lazy val connector = project
  .settings(name := "quasar-connector-internal")
  .dependsOn(
    api,
    sql % "test->test",
    qsu)
  .settings(commonSettings)
  .settings(publishTestsSettings)
  .settings(targetSettings)
  .settings(excludeTypelevelScalaLibrary)
  .settings(
    libraryDependencies ++= Dependencies.connector)
  .enablePlugins(AutomateHeaderPlugin)

lazy val core = project
  .settings(name := "quasar-core-internal")
  .dependsOn(
    connector % BothScopes,
    sql,
    effect    % "test->test",
    fs        % "test->test")
  .settings(commonSettings)
  .settings(publishTestsSettings)
  .settings(targetSettings)
  .settings(
    libraryDependencies ++= Dependencies.core)
  .settings(excludeTypelevelScalaLibrary)
  .enablePlugins(AutomateHeaderPlugin)

/** Implementation of the Couchbase connector.
  */
lazy val couchbase = project
  .settings(name := "quasar-couchbase-internal")
  .dependsOn(
    connector % BothScopes,
    qscript   % "test->test")
  .settings(commonSettings)
  .settings(targetSettings)
  .settings(libraryDependencies ++= Dependencies.couchbase)
  .settings(isolatedBackendSettings("quasar.physical.couchbase.Couchbase$"))
  .settings(excludeTypelevelScalaLibrary)
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
  .settings(isolatedBackendSettings("quasar.physical.marklogic.MarkLogic$"))
  .settings(excludeTypelevelScalaLibrary)
  .enablePlugins(AutomateHeaderPlugin)

/** Implementation of the MongoDB connector.
  */
lazy val mongodb = project
  .settings(name := "quasar-mongodb-internal")
  .dependsOn(
    fs        % "test->test",
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
  .settings(isolatedBackendSettings("quasar.physical.mongodb.MongoDb$"))
  .settings(excludeTypelevelScalaLibrary)
  .enablePlugins(AutomateHeaderPlugin)

/** A connector outline, meant to be copied and incrementally filled in while
  * implementing a new connector.
  */
lazy val skeleton = project
  .settings(name := "quasar-skeleton-internal")
  .dependsOn(connector % BothScopes)
  .settings(commonSettings)
  .settings(targetSettings)
  .settings(excludeTypelevelScalaLibrary)
  .enablePlugins(AutomateHeaderPlugin)

/** Types and operations needed by applications that embed Quasar.
  */
lazy val interface = project
  .settings(name := "quasar-interface-internal")
  .dependsOn(
    core % BothScopes,
    skeleton,
    mimir)
  .settings(commonSettings)
  .settings(publishTestsSettings)
  .settings(targetSettings)
  .settings(libraryDependencies ++= Dependencies.interface)
  .settings(excludeTypelevelScalaLibrary)
  .enablePlugins(AutomateHeaderPlugin)

/** An interactive REPL application for Quasar.
  */
lazy val repl = project
  .settings(name := "quasar-repl")
  .dependsOn(interface)
  .settings(commonSettings)
  .settings(targetSettings)
  .settings(backendRewrittenRunSettings)
  .settings(
    fork in run := true,
    connectInput in run := true,
    outputStrategy := Some(StdoutOutput))
  .settings(excludeTypelevelScalaLibrary)
  .enablePlugins(AutomateHeaderPlugin)

/** An HTTP interface to Quasar.
  */
lazy val web = project
  .settings(name := "quasar-web")
  .dependsOn(interface % BothScopes)
  .settings(commonSettings)
  .settings(publishTestsSettings)
  .settings(targetSettings)
  .settings(backendRewrittenRunSettings)
  .settings(
    mainClass in Compile := Some("quasar.server.Server"),
    libraryDependencies ++= Dependencies.web)
  .settings(excludeTypelevelScalaLibrary)
  .enablePlugins(AutomateHeaderPlugin)

/** Integration tests that have some dependency on a running connector.
  */
lazy val it = project
  .settings(name := "quasar-it-internal")
  .configs(ExclusiveTests)
  .dependsOn(
    web     % BothScopes,
    qscript % "test->test")
  .settings(commonSettings)
  .settings(publishTestsSettings)
  .settings(targetSettings)
  .settings(libraryDependencies ++= Dependencies.it)
  // Configure various test tasks to run exclusively in the `ExclusiveTests` config.
  .settings(inConfig(ExclusiveTests)(Defaults.testTasks): _*)
  .settings(inConfig(ExclusiveTests)(exclusiveTasks(test, testOnly, testQuick)): _*)
  .settings(parallelExecution in Test := false)
  .settings(
    sideEffectTestFSConfig := {
      val LoadCfgProp = "slamdata.internal.fs-load-cfg"

      val parentCp = (fullClasspath in connector in Compile).value.files
      val backends = isolatedBackends.value map {
        case (name, childCp) =>
          val classpathStr =
            createBackendEntry(childCp, parentCp).map(_.getAbsolutePath).mkString(":")

          name + "=" + classpathStr
      }

      if (java.lang.System.getProperty(LoadCfgProp, "").isEmpty) {
        // we aren't forking tests, so we just set the property in the current JVM
        java.lang.System.setProperty(LoadCfgProp, backends.mkString(";"))
      }

      ()
    },

    test := Def.taskDyn {
      val _ = sideEffectTestFSConfig.value

      test in Test
    }.value)
  .settings(excludeTypelevelScalaLibrary)
  .enablePlugins(AutomateHeaderPlugin)

lazy val marklogicIt = project
  .configs(ExclusiveTests)
  .dependsOn(it % BothScopes, marklogic % BothScopes)
  .settings(commonSettings)
  .settings(noPublishSettings)
  .settings(targetSettings)
  // Configure various test tasks to run exclusively in the `ExclusiveTests` config.
  .settings(inConfig(ExclusiveTests)(Defaults.testTasks): _*)
  .settings(inConfig(ExclusiveTests)(exclusiveTasks(test, testOnly, testQuick)): _*)
  .settings(parallelExecution in Test := false)
  .settings(excludeTypelevelScalaLibrary)
  .enablePlugins(AutomateHeaderPlugin)

lazy val mongoIt = project
  .configs(ExclusiveTests)
  .dependsOn(it % BothScopes, mongodb)
  .settings(commonSettings)
  .settings(noPublishSettings)
  .settings(targetSettings)
  // Configure various test tasks to run exclusively in the `ExclusiveTests` config.
  .settings(inConfig(ExclusiveTests)(Defaults.testTasks): _*)
  .settings(inConfig(ExclusiveTests)(exclusiveTasks(test, testOnly, testQuick)): _*)
  .settings(parallelExecution in Test := false)
  .settings(excludeTypelevelScalaLibrary)
  .enablePlugins(AutomateHeaderPlugin)

lazy val couchbaseIt = project
  .configs(ExclusiveTests)
  .dependsOn(it % BothScopes, couchbase)
  .settings(commonSettings)
  .settings(noPublishSettings)
  .settings(targetSettings)
  // Configure various test tasks to run exclusively in the `ExclusiveTests` config.
  .settings(inConfig(ExclusiveTests)(Defaults.testTasks): _*)
  .settings(inConfig(ExclusiveTests)(exclusiveTasks(test, testOnly, testQuick)): _*)
  .settings(parallelExecution in Test := false)
  .settings(excludeTypelevelScalaLibrary)
  .enablePlugins(AutomateHeaderPlugin)

/***** PRECOG *****/

import precogbuild.Build._

lazy val precog = project.setup
  .settings(name := "quasar-precog-internal")
  .dependsOn(common % BothScopes)
  .withWarnings
  .settings(libraryDependencies ++= Dependencies.precog)
  .settings(headerLicenseSettings)
  .settings(publishSettings)
  .settings(assemblySettings)
  .settings(targetSettings)
  .settings(excludeTypelevelScalaLibrary)
  .enablePlugins(AutomateHeaderPlugin)

lazy val blueeyes = project.setup
  .settings(name := "quasar-blueeyes-internal")
  .dependsOn(precog % BothScopes, frontend)
  .withWarnings
  .settings(libraryDependencies ++= Dependencies.blueeyes)
  .settings(headerLicenseSettings)
  .settings(publishSettings)
  .settings(assemblySettings)
  .settings(targetSettings)
  .settings(excludeTypelevelScalaLibrary)
  .enablePlugins(AutomateHeaderPlugin)

lazy val niflheim = project.setup
  .settings(name := "quasar-niflheim-internal")
  .dependsOn(blueeyes % BothScopes)
  .scalacArgs("-Ypartial-unification")
  .withWarnings
  .settings(libraryDependencies ++= Dependencies.niflheim)
  .settings(headerLicenseSettings)
  .settings(publishSettings)
  .settings(assemblySettings)
  .settings(targetSettings)
  .settings(excludeTypelevelScalaLibrary)
  .enablePlugins(AutomateHeaderPlugin)

lazy val yggdrasil = project.setup
  .settings(name := "quasar-yggdrasil-internal")
  .dependsOn(niflheim % BothScopes)
  .withWarnings
  .settings(
    resolvers += "bintray-djspiewak-maven" at "https://dl.bintray.com/djspiewak/maven",
    libraryDependencies ++= Dependencies.yggdrasil)
  .settings(headerLicenseSettings)
  .settings(publishSettings)
  .settings(assemblySettings)
  .settings(targetSettings)
  .settings(excludeTypelevelScalaLibrary)
  .enablePlugins(AutomateHeaderPlugin)

lazy val mimir = project.setup
  .settings(name := "quasar-mimir-internal")
  .dependsOn(
    yggdrasil % BothScopes,
    connector)
  .scalacArgs("-Ypartial-unification")
  .withWarnings
  .settings(libraryDependencies ++= Dependencies.mimir)
  .settings(headerLicenseSettings)
  .settings(publishSettings)
  .settings(assemblySettings)
  .settings(targetSettings)
  .settings(excludeTypelevelScalaLibrary)
  .enablePlugins(AutomateHeaderPlugin)

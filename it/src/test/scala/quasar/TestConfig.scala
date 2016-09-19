/*
 * Copyright 2014–2016 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar

import quasar.Predef._
import quasar.contrib.pathy._
import quasar.fs._
import quasar.fs.mount.MountConfig

import argonaut._
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent._

object TestConfig {

  /** The directory under which test data may be found as well as where tests
    * can write test data/output.
    */
  val DefaultTestPrefix: ADir = rootDir </> dir("quasar-test")

  /** The environment variable used to externally specify the test path prefix.
    *
    * NB: The same path prefix is used for all backends under test.
    */
  val TestPathPrefixEnvName = "QUASAR_TEST_PATH_PREFIX"

  /** External Backends. */
  val MONGO_2_6       = BackendName("mongodb_2_6")
  val MONGO_3_0       = BackendName("mongodb_3_0")
  val MONGO_3_2       = BackendName("mongodb_3_2")
  val MONGO_READ_ONLY = BackendName("mongodb_read_only")
  val SKELETON = BackendName("skeleton")
  val POSTGRESQL = BackendName("postgresql")
  val SPARK_LOCAL = BackendName("spark_local")
  val MARKLOGIC       = BackendName("marklogic")

  lazy val backendNames: List[BackendName] =
    List(MONGO_2_6, MONGO_3_0, MONGO_3_2, MONGO_READ_ONLY, SKELETON, POSTGRESQL, SPARK_LOCAL, MARKLOGIC)

  final case class UnsupportedFileSystemConfig(c: MountConfig)
    extends RuntimeException(s"Unsupported filesystem config: $c")

  /** True if this backend configuration is for a mongo connection where the
    * user has the "read-only" role.
    */
  def isMongoReadOnly(backendName: BackendName): Boolean =
    backendName == MONGO_READ_ONLY

  /** Returns the name of the environment variable used to configure the
    * given backend.
    */
  def backendEnvName(backendName: BackendName): String =
    "QUASAR_" + backendName.name.toUpperCase

  /** The name of the environment variable to configure the insert connection
    * for a read-only backend.
    */
  def insertEnvName(b: BackendName) = backendEnvName(b) + "_INSERT"

  /** Returns the list of filesystems to test, using the provided function
    * to select an interpreter for a given config.
    */
  def externalFileSystems[S[_]](
    pf: PartialFunction[(MountConfig, ADir), Task[(S ~> Task, Task[Unit])]]
  ): Task[IList[FileSystemUT[S]]] = {
    def fs(
      envName: String,
      p: ADir
    ): OptionT[Task, Task[(S ~> Task, Task[Unit])]] =
      TestConfig.loadConfig(envName) flatMapF (c =>
        pf.lift((c, p)).cata(
          Task.delay(_),
          Task.fail(new UnsupportedFileSystemConfig(c))))

    def fileSystemNamed(n: BackendName, p: ADir): OptionT[Task, FileSystemUT[S]] = {
      def rsrc(connect: Task[(S ~> Task, Task[Unit])]): Task[TaskResource[(S ~> Task, Task[Unit])]] =
        TaskResource(connect, Strategy.DefaultStrategy)(_._2)

      // Put the evaluation of a Task to produce an interpreter _into_ the interpreter:
      def embed(t: Task[S ~> Task]): S ~> Task = new (S ~> Task) {
        def apply[A](a: S[A]): Task[A] =
          t.flatMap(_(a))
      }

      for {
        test     <- fs(backendEnvName(n), p)
        setup    <- fs(insertEnvName(n), p).run.liftM[OptionT]
        s        <- NameGenerator.salt.liftM[OptionT]
        testRef  <- rsrc(test).liftM[OptionT]
        setupRef <- setup.cata(rsrc, Task.now(testRef)).liftM[OptionT]
      } yield FileSystemUT(n,
          embed(testRef.get.map(_._1)),
          embed(setupRef.get.map(_._1)),
          p </> dir(s),
          testRef.release *> setupRef.release)
    }

    def noBackendsFound: Throwable = new RuntimeException(
      "No external backends to test. Consider setting one of these environment variables: " +
      TestConfig.backendNames.map(TestConfig.backendEnvName).mkString(", ")
    )

    TestConfig.testDataPrefix flatMap { prefix =>
      TestConfig.backendNames.toIList
        .traverse(n => fileSystemNamed(n, prefix).run)
        .map(_.unite)
        .flatMap(uts => if (uts.isEmpty) Task.fail(noBackendsFound) else Task.now(uts))
    }
  }

  /** Load backend config from environment variable.
    *
    * Fails if it cannot parse the config and returns None if there is no config.
    */
  def loadConfig(envName: String): OptionT[Task, MountConfig] =
    console.readEnv(envName).flatMapF(value =>
      Parse.decodeEither[MountConfig](value).fold(
        e => fail("Failed to parse $" + envName + ": " + e),
        _.point[Task]))

  /** Load a pair of backend configs, the first for inserting test data, and
    * the second for actually running tests. If no config is specified for
    * inserting, then the test config is just returned twice.
    */
  def loadConfigPair(name: BackendName): OptionT[Task, (MountConfig, MountConfig)] = {
    OptionT((loadConfig(insertEnvName(name)).run |@| loadConfig(backendEnvName(name)).run) { (c1, c2) =>
      c2.map(c2 => (c1.getOrElse(c2), c2))
    })
  }

  /** Returns the absolute path within a filesystem to the directory where tests
    * may write data.
    *
    * One may specify this externally by setting the [[TestPathPrefixEnvName]].
    * The returned [[Task]] will fail if an invalid path is provided from the
    * environment and return the [[DefaultTestPrefix]] if nothing is provided.
    */
  def testDataPrefix: Task[ADir] =
    console.readEnv(TestPathPrefixEnvName) flatMap { s =>
      posixCodec.parseAbsDir(s).cata(
        d => OptionT(sandbox(rootDir, d).map(rootDir </> _).point[Task]),
        fail[ADir](s"Test data dir must be an absolute dir, got: $s").liftM[OptionT])
    } getOrElse DefaultTestPrefix

  ////

  private def fail[A](msg: String): Task[A] = Task.fail(new RuntimeException(msg))
}

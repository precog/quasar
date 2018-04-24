/*
 * Copyright 2014–2018 SlamData Inc.
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

import slamdata.Predef._
import quasar.contrib.scalaz._
import quasar.contrib.pathy._
import quasar.effect.NameGenerator
import quasar.fs._
import quasar.fs.mount.{BackendDef, ConnectionUri, MountConfig}
import quasar.main.BackendConfig

import knobs.{Required, Optional, FileResource}
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent._

object TestConfig {

  /** The directory under which test data may be found as well as where tests
    * can write test data/output.
    */
  val DefaultTestPrefix: ADir = rootDir </> dir("t")

  /** The environment variable used to externally specify the test path prefix.
    *
    * NB: The same path prefix is used for all backends under test.
    */
  val TestPathPrefixEnvName = "QUASAR_TEST_PATH_PREFIX"

  /**
   * External Backends.
   *
   * This is an artifact of the fact that we haven't inverted the dependency between
   * `it` and the connectors.  Hence, the redundant hard-coding of constants.  We
   * should get rid of this abomination as soon as possible.
   */
  val COUCHBASE       = ExternalBackendRef(BackendRef(BackendName("couchbase")        , BackendCapability.All), FileSystemType("couchbase"))
  val MARKLOGIC_JSON  = ExternalBackendRef(BackendRef(BackendName("marklogic_json")   , BackendCapability.All), FileSystemType("marklogic"))
  val MARKLOGIC_XML   = ExternalBackendRef(BackendRef(BackendName("marklogic_xml")    , BackendCapability.All), FileSystemType("marklogic"))
  val MIMIR           = ExternalBackendRef(BackendRef(BackendName("mimir")            , BackendCapability.All), mimir.Mimir.Type)
  val MONGO_3_2       = ExternalBackendRef(BackendRef(BackendName("mongodb_3_2")      , BackendCapability.All), FileSystemType("mongodb"))
  val MONGO_3_4       = ExternalBackendRef(BackendRef(BackendName("mongodb_3_4")      , BackendCapability.All), FileSystemType("mongodb"))
  val MONGO_3_4_13    = ExternalBackendRef(BackendRef(BackendName("mongodb_3_4_13")   , BackendCapability.All), FileSystemType("mongodb"))
  val MONGO_3_6       = ExternalBackendRef(BackendRef(BackendName("mongodb_3_6")      , BackendCapability.All), FileSystemType("mongodb"))
  val MONGO_READ_ONLY = ExternalBackendRef(BackendRef(BackendName("mongodb_read_only"), ISet singleton BackendCapability.query()), FileSystemType("mongodb"))

  lazy val backendRefs: List[ExternalBackendRef] = List(
    COUCHBASE,
    MARKLOGIC_JSON, MARKLOGIC_XML,
    MIMIR,
    MONGO_3_2, MONGO_3_4, MONGO_3_4_13, MONGO_3_6, MONGO_READ_ONLY)

  final case class UnsupportedFileSystemConfig(c: MountConfig)
    extends RuntimeException(s"Unsupported filesystem config: $c")

  /** True if this backend configuration is for a couchbase connection.
    */
  def isCouchbase(backendRef: BackendRef): Boolean =
    backendRef === COUCHBASE.ref

  /** Returns the name of the environment variable used to configure the
    * given backend.
    */
  def backendConfName(backendName: BackendName): String =
    backendName.name

  /** The name of the configuration parameter that points to uri that should be
    *  used for inserting
    */
  def insertConfName(b: BackendName) = backendConfName(b) + "_insert"

  /** Returns the list of filesystems to test, using the provided function
    * to select an interpreter for a given config.
    */
  def externalFileSystems[S[_]](
    pf: PartialFunction[BackendDef.FsCfg, Task[(S ~> Task, Task[Unit])]]
  ): Task[IList[SupportedFs[S]]] = {
    def fs(
      envName: String,
      typ: FileSystemType
    ): OptionT[Task, Task[(S ~> Task, Task[Unit])]] =
      TestConfig.loadConnectionUri(envName) flatMapF { uri =>
        pf.lift((typ, uri)).cata(
          Task.delay(_),
          Task.fail(new UnsupportedFileSystemConfig(MountConfig.fileSystemConfig(typ, uri))))
      }

    def lookupFileSystem(r: ExternalBackendRef, p: ADir): OptionT[Task, FileSystemUT[S]] = {
      def rsrc(connect: Task[(S ~> Task, Task[Unit])]): Task[TaskResource[(S ~> Task, Task[Unit])]] =
        TaskResource(connect, Strategy.DefaultStrategy)(_._2)

      // Put the evaluation of a Task to produce an interpreter _into_ the interpreter:
      def embed(t: Task[S ~> Task]): S ~> Task = new (S ~> Task) {
        def apply[A](a: S[A]): Task[A] =
          t.flatMap(_(a))
      }

      for {
        test     <- fs(backendConfName(r.ref.name), r.fsType)
        setup    <- fs(insertConfName(r.ref.name), r.fsType).run.liftM[OptionT]
        s        <- NameGenerator.salt.liftM[OptionT]
        testRef  <- rsrc(test).liftM[OptionT]
        setupRef <- setup.cata(rsrc, Task.now(testRef)).liftM[OptionT]
      } yield FileSystemUT(r.ref,
          embed(testRef.get.map(_._1)),
          embed(setupRef.get.map(_._1)),
          p </> dir("run" + s),
          testRef.release *> setupRef.release)
    }

    TestConfig.testDataPrefix flatMap { prefix =>
      TestConfig.backendRefs.toIList
        .traverse(r => lookupFileSystem(r, prefix).run.map(fsUT => SupportedFs(r.ref,fsUT, fsUT.map(_.copy(testDir = rootDir)))))
    }
  }

  /** Loads all the configurations for a particular type of FileSystem. */
  def fileSystemConfigs(tpe: FileSystemType): Task[List[(BackendRef, ConnectionUri, ConnectionUri)]] =
    backendRefs.filter(_.fsType === tpe).foldMapM(r => TestConfig.loadConnectionUriPair(r.name).run map (_.toList map {
      case (testUri, setupUri) => (r.ref, testUri, setupUri)
    }))

  val confFile: String = "it/testing.conf"
  val defaultConfFile: String = "it/testing.conf.example"

  def confValue(name: String): OptionT[Task, String] = {
    val config = knobs.loadImmutable(
      Optional(FileResource(new java.io.File(confFile)))        ::
      Required(FileResource(new java.io.File(defaultConfFile))) ::
      Nil)
    OptionT(config.map(_.lookup[String](name)))
  }

  /** Load backend config from environment variable.
    *
    * Fails if it cannot parse the config and returns None if there is no config.
    */
  def loadConnectionUri(name: String): OptionT[Task, ConnectionUri] =
    confValue(name).map(ConnectionUri(_))

  def loadConnectionUri(ref: BackendRef): OptionT[Task, ConnectionUri] =
    loadConnectionUri(backendConfName(ref.name))

  /** Load a pair of backend configs, the first for inserting test data, and
    * the second for actually running tests. If no config is specified for
    * inserting, then the test config is just returned twice.
    */
  def loadConnectionUriPair(name: BackendName): OptionT[Task, (ConnectionUri, ConnectionUri)] = {
    OptionT((loadConnectionUri(insertConfName(name)).run |@| loadConnectionUri(backendConfName(name)).run) { (c1, c2) =>
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

  val testBackendConfig: Task[BackendConfig] = {
    val confStrM =
      Task.delay(java.lang.System.getProperty("slamdata.internal.fs-load-cfg", ""))

    confStrM flatMap { confStr =>
      import java.io.File

      val backends =
        if (confStr.isEmpty) IList.empty[(scala.Predef.String, scala.collection.Seq[java.io.File])]
        else {
          IList(confStr.split(";"): _*) map { backend =>
            val List(name, classpath) = backend.split("=").toList

            name -> classpath.split(":").map(new File(_)).toSeq
          }
        }

      BackendConfig.fromBackends(backends)
    }
  }

  ////

  private def fail[A](msg: String): Task[A] = Task.fail(new RuntimeException(msg))
}

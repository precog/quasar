/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.server

import slamdata.Predef._
import quasar.cli.Cmd.Start
import quasar.config.{ConfigOps, FsPath, WebConfig}
import quasar.contrib.pathy._
import quasar.db.{DbUtil, StatefulTransactor}
import quasar.fs.mount._
import quasar.main._, metastore._
import quasar.metastore._, MetaStoreAccess._
import quasar.server.Server.QuasarConfig
import quasar.sql._
import quasar.TestConfig

import java.io.File
import scala.util.Random.nextInt

import argonaut._, Argonaut._
import doobie.imports._
import org.http4s.{Query => _, _}, Status._, Uri.Authority
import org.http4s.argonaut._
import org.specs2.execute.{AsResult, Result}
import org.specs2.matcher.MatchResult
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

class ServiceSpec extends quasar.Qspec {
  val schema = Schema.schema

  val configOps = ConfigOps[WebConfig]

  val client = org.http4s.client.blaze.defaultClient

  sequential

  def withServer[A]
    (port: Int = 8888, metastoreInit: ConnectionIO[Unit] = ().η[ConnectionIO])
    (f: Uri => Task[A])
    : String \/ A = {
    val uri = Uri(authority = Some(Authority(port = Some(port))))

    (for {
      cfgPath       <- FsPath.parseSystemFile(
                         File.createTempFile("quasar", ".json").toString
                       ).run.liftM[MainErrT]
      qCfg          =  QuasarConfig(
                         cmd = Start,
                         staticContent = Nil,
                         redirect = None,
                         port = None,
                         configPath = cfgPath,
                         openClient = false)
      transactor    <- Task.delay(DbUtil.simpleTransactor(
                         DbUtil.inMemoryConnectionInfo(s"test_mem_service_spec_$nextInt")
                       )).liftM[MainErrT]
      _             <- schema.updateToLatest.transact(transactor).liftM[MainErrT]
      _             <- metastoreInit.transact(transactor).liftM[MainErrT]
      msCtx         <- metastoreCtx(StatefulTransactor(transactor, Task.now(())))
      (svc, close)  =  Server.durableService(qCfg, port, msCtx)
      (p, shutdown) <- Http4sUtils.startServers(port, svc).liftM[MainErrT]
      r             <- f(uri)
                          .onFinish(_ => shutdown)
                          .onFinish(_ => p.run)
                          .onFinish(_ => close)
                          .liftM[MainErrT]
    } yield r).run.unsafePerformSync
  }

  "/mount/fs" should {

    "POST view" in {
      val port = Http4sUtils.anyAvailablePort.unsafePerformSync

      val r = withServer(port) { baseUri: Uri =>
        client.fetch(
          Request(
              uri = baseUri / "mount" / "fs",
              method = Method.POST,
              headers = Headers(Header("X-File-Name", "a")))
            .withBody("""{ "view": { "connectionUri" : "sql2:///?q=%28select%201%29" } }""")
          )(Task.now) *>
        client.fetch(
          Request(
            uri = baseUri / "mount" / "fs" / "a",
            method = Method.GET)
          )(Task.now)
      }

      r.map(_.status) must beRightDisjunction(Ok)
    }

    "PUT view" in {
      val port = Http4sUtils.anyAvailablePort.unsafePerformSync

      val r = withServer(port) { baseUri: Uri =>
        client.fetch(
          Request(
              uri = baseUri / "mount" / "fs" / "a",
              method = Method.PUT)
            .withBody("""{ "view": { "connectionUri" : "sql2:///?q=%28select%201%29" } }""")
          )(Task.now) *>
        client.fetch(
          Request(
            uri = baseUri / "mount" / "fs" / "a",
            method = Method.GET)
          )(Task.now)
      }

      r.map(_.status) must beRightDisjunction(Ok)
    }

    "[SD-1833] replace view" in {
      val port = Http4sUtils.anyAvailablePort.unsafePerformSync
      val sel1 = "sql2:///?q=%28select%201%29"
      val sel2 = "sql2:///?q=%28select%202%29"

      val finalCfg = MountConfig.viewConfig0(sqlB"select 2")

      val r = withServer(port) { baseUri: Uri =>
        client.fetch(
          Request(
              uri = baseUri / "mount" / "fs" / "viewA",
              method = Method.PUT)
            .withBody(s"""{ "view": { "connectionUri" : "$sel1" } }""")
          )(Task.now) *>
        client.fetch(
          Request(
              uri = baseUri / "mount" / "fs" / "viewA",
              method = Method.PUT)
            .withBody(s"""{ "view": { "connectionUri" : "$sel2" } }""")
          )(Task.now) *>
        client.expect[Json](baseUri / "mount" / "fs" / "viewA")
      }

      r ==== finalCfg.asJson.right
    }

    "MOVE view" in {
      val port = Http4sUtils.anyAvailablePort.unsafePerformSync

      val srcPath = rootDir </> dir("view") </> file("a")
      val dstPath = rootDir </> dir("view") </> file("b")
      val viewConfig = MountConfig.viewConfig0(sqlB"select * from zips")

      val insertMnts = insertMount(srcPath, viewConfig)

      val r = withServer(port, insertMnts) { baseUri: Uri =>
        client.fetch(
          Request(
            uri = baseUri / "mount" / "fs" / "view" / "a",
            method = Method.MOVE,
            headers = Headers(Header("Destination", UriPathCodec.printPath(dstPath))))
          )(Task.now) *>
        client.fetch(
          Request(
            uri = baseUri / "mount" / "fs" / "view" / "b",
            method = Method.GET)
          )(Task.now)
      }

      r.map(_.status) must beRightDisjunction(Ok)
    }
  }

  "/data/fs" should {
    val fileSystemConfigs =
      TestConfig.backendRefs
        .traverse { ref =>
          val connectionUri = TestConfig.loadConnectionUri(ref.ref)
          connectionUri.map(MountConfig.fileSystemConfig(ref.fsType, _)).run
        }.map(_
          .unite
          .zipWithIndex
          .map { case (c, i) => (rootDir </> dir("data") </> dir(i.toString)) -> c }
          .toMap[APath, MountConfig])
        .unsafePerformSync

    val testName = "MOVE view"

    def withFileSystemConfigs[A](result: MatchResult[A]): Result =
      fileSystemConfigs.isEmpty.fold(
        skipped("Warning: no test backends enabled"),
        AsResult(result))

    "MOVE view" in withFileSystemConfigs {
      val port = Http4sUtils.anyAvailablePort.unsafePerformSync

      val srcPath = rootDir </> dir("view") </> file("a")
      val dstPath = rootDir </> dir("view") </> file("b")

      val viewConfig = MountConfig.viewConfig0(sqlB"select 42")

      val insertMnts =
        insertMount(srcPath, viewConfig) <*
        fileSystemConfigs.toList.traverse {
          case (p, m) => insertMount(p, m)
        }

      val r = withServer(port, insertMnts) { baseUri: Uri =>
        client.fetch(
          Request(
            uri = baseUri / "data" / "fs" / "view" / "a",
            method = Method.MOVE,
            headers = Headers(Header("Destination", UriPathCodec.printPath(dstPath))))
          )(Task.now) *>
        client.fetch(
          Request(
            uri = baseUri / "data" / "fs" / "view" / "b",
            method = Method.GET)
          )(Task.now)
      }

      r.map(_.status) must beRightDisjunction(Ok)
    }

    "MOVE a directory containing views and files" in withFileSystemConfigs {
      val port = Http4sUtils.anyAvailablePort.unsafePerformSync

      val srcPath = rootDir </> dir("a")
      val dstPath = rootDir </> dir("b")

      val viewConfig = MountConfig.viewConfig0(sqlB"select 42")

      val insertMnts =
        insertMount(srcPath </> file("view"), viewConfig) <*
        fileSystemConfigs.toList.traverse { case (p, m) => insertMount(p, m) }

      val r = withServer(port, insertMnts) { baseUri: Uri =>
        client.fetch(
          Request(
            uri = baseUri / "data" / "fs" / "a" / "",
            method = Method.MOVE,
            headers = Headers(Header("Destination", UriPathCodec.printPath(dstPath))))
          )(Task.now) *>
        client.fetch(
          Request(
            uri = baseUri / "data" / "fs" / "b" / "",
            method = Method.GET)
          )(Task.now)
      }

      r.map(_.status) must beRightDisjunction(Ok)
    }
  }

  step(client.shutdown.unsafePerformSync)

}

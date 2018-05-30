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

package quasar.server

import slamdata.Predef._
import quasar.config.{ConfigOps, WebConfig}
import quasar.contrib.pathy._
import quasar.fp.TaskRef
import quasar.fp.ski._
import quasar.fs.mount._
import quasar.main._
import quasar.metastore._, MetaStoreAccess._
import quasar.metastore.MetaStoreFixture.createNewTestMetastore
import quasar.sql._
import quasar.TestConfig

import argonaut._, Argonaut._
import doobie.imports._
import org.http4s.{Query => _, _}, Status._, Uri.Authority
import org.http4s.argonaut._
import org.specs2.execute.{AsResult, Result}
import org.specs2.matcher.MatchResult
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import eu.timepit.refined.refineMV

class ServiceSpec extends quasar.Qspec {
  val schema = Schema.schema

  val configOps = ConfigOps[WebConfig]

  val client = org.http4s.client.blaze.defaultClient

  sequential // These tests spin up a server and there is potential for port conflicts if
             // they don't run sequentially

  def withServer[A]
    (port: Int = 8888, metastoreInit: ConnectionIO[Unit] = ().η[ConnectionIO])
    (f: Uri => Task[A])
    : String \/ A = {
    val uri = Uri(authority = Some(Authority(port = Some(port))))

    (for {
      metastore  <- createNewTestMetastore().liftM[MainErrT]
      transactor = metastore.trans.transactor
      _          <- schema.updateToLatest.transact(transactor).liftM[MainErrT]
      _          <- metastoreInit.transact(transactor).liftM[MainErrT]
      metaRef    <- TaskRef(metastore).liftM[MainErrT]
      quasarFs   <- Quasar.initWithMeta(BackendConfig.Empty, metaRef, _ => ().point[MainTask])
      shutdown   <- Server.startServer(quasarFs.interp, port, Nil, None, _ => ().point[MainTask], refineMV(0L)).liftM[MainErrT]
      r          <- f(uri).onFinish(κ(shutdown.onFinish(κ(quasarFs.shutdown)))).liftM[MainErrT]
    } yield r).run.unsafePerformSync
  }

  /*
   * TODO collapse this a bit
   *
   * This code dates back to a time when we tested everything in the
   * filesystem config.  Now, we just test against mimir (here).  This
   * code should be collapsed and inlined, and the tests re-specialized
   * to a single backend (mimir).  I'm just in a hurry right now...
   */
  val fileSystemConfigs: Map[APath, MountConfig.FileSystemConfig] =
    List(TestConfig.MIMIR)
      .traverse { ref =>
        val connectionUri = TestConfig.loadConnectionUri(ref.ref)
        connectionUri.map(MountConfig.FileSystemConfig(ref.fsType, _)).run
      }.map(_
        .unite
        .zipWithIndex
        .map { case (c, i) => (rootDir </> dir("data") </> dir(i.toString)) -> c }
        .toMap[APath, MountConfig.FileSystemConfig])
      .unsafePerformSync

  def withFileSystemConfigs[A](result: => MatchResult[A]): Result =
    fileSystemConfigs.isEmpty.fold(
      skipped("Warning: no test backends enabled"),
      AsResult(result))

  "/mount/fs" should {

    "mount filesystem" in withFileSystemConfigs {
      val port = Http4sUtils.anyAvailablePort.unsafePerformSync

      val r  = withServer(port) { baseUri: Uri =>
        fileSystemConfigs.toList.traverse { case (f,c) =>
          client.fetch(
            Request(
                uri = baseUri / "mount" / "fs",
                method = Method.POST,
                headers = Headers(Header("X-File-Name", c.typ.value + "/")))
              .withBody(s"""{ "${c.typ.value}": { "connectionUri" : "${c.uri.value.replace("\\", "\\\\")}" } }""")
            )(_.body.run) *>
          client.fetch(
            Request(
              uri = baseUri / "mount" / "fs" / c.typ.value / "",
              method = Method.GET)
            )(r => r.body.run.map(_ => r.status))
        }
      }

      r must beLike {
        case \/-(statii) =>
          statii must contain((s: Status) => s mustEqual Ok).foreach
      }
    }

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

      r.map(_.status) must be_\/-(Ok)
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

      r.map(_.status) must be_\/-(Ok)
    }

    "[SD-1833] replace view" in withFileSystemConfigs {
      val port = Http4sUtils.anyAvailablePort.unsafePerformSync
      val sel1 = "sql2:///?q=%28select%201%29"
      val sel2 = "sql2:///?q=%28select%202%29"

      val finalCfg = MountConfig.viewConfig0(sqlB"select 2")

      val mnts =
        fileSystemConfigs.headOption.traverse { case (_, m) => insertMount(rootDir, m) }.void

      val r = withServer(port, mnts) { baseUri: Uri =>
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
    }.flakyTest("this test is actually non-deterministic depending on server scheduling")

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

      r.map(_.status) must be_\/-(Ok)
    }
  }

  "/data/fs" should {

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

      r.map(_.status) must be_\/-(Ok)
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

      r.map(_.status) must be_\/-(Ok)
    }

    "GET invalid view" in {
      val port = Http4sUtils.anyAvailablePort.unsafePerformSync

      val insertMnt =
        MetaStoreMounterSpec.insertMount(rootDir </> file("f1"), MountType.ViewMount, "bogus")

      val r = withServer(port, insertMnt) { baseUri: Uri =>
        client.fetch(
          Request(
            uri = baseUri / "data" / "fs" / "f1",
            method = Method.GET)
        )(Task.now)
      }

      r.map(_.status) must be_\/-(InternalServerError withReason "Failed to plan query.")
    }
  }

  "/metadata/fs" should {
    "GET directory with invalid view" in {
      import Json._

      val port = Http4sUtils.anyAvailablePort.unsafePerformSync

      val viewConfig = MountConfig.viewConfig0(sqlB"select 42")

      val insertMnts =
        insertMount(rootDir </> file("f1"), viewConfig) *>
        MetaStoreMounterSpec.insertMount(rootDir </> file("f2"), MountType.ViewMount, "bogus")

      val r = withServer(port, insertMnts) { baseUri: Uri =>
        client.expect[Json](baseUri / "metadata" / "fs")
      }

      r.map(json =>
        (json.hcursor
          --\ "children"
          -\ (c => (c.hcursor --\ "name").as[String].toOption ≟ "f2".some)
          --\ "mount"
          --\ "error"
          --\ "detail"
          --\ "message" := jEmptyString
        ).up.up.up.up.up.focus >>= (_.array ∘ (_.toSet))
      ) must be_\/-(
        Set(
          Json("name" := "f1", "type" := "file", "mount" := "view"),
          Json(
            "name" := "f2",
            "type" := "view",
            "mount" := Json(
              "error" := Json(
                "status" := "Invalid mount.",
                "detail" := Json("message" := ""))))).some)
    }
  }

  step(client.shutdown.unsafePerformSync)

}

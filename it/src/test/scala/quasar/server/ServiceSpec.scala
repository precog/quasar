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

package quasar.server

import quasar.Predef._
import quasar.config.{ConfigOps, FsPath, WebConfig}
import quasar.main.MainErrT
import quasar.api.UriPathCodec
import quasar.fp._
import quasar.fs._, mount._
import quasar.server.Server.QuasarConfig
import quasar.TestConfig

import java.io.File

import matryoshka.Mu
import org.http4s.Uri.Authority
import org.http4s._, Status._
import org.specs2.mutable
import org.specs2.scalaz.DisjunctionMatchers._
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

class ServiceSpec extends mutable.Specification {
  implicit val configOps: ConfigOps[WebConfig] = WebConfig

  val client = org.http4s.client.blaze.defaultClient

  def withServer[A]
    (port: Int = 8888, webConfig: WebConfig = WebConfig.default)
    (f: Uri => Task[A])
    : String \/ A = {
    val uri = Uri(authority = Some(Authority(port = Some(port))))

    val service = Server.durableService[Mu](
      QuasarConfig(
        staticContent = Nil,
        redirect = None,
        port = None,
        configPath = FsPath.parseSystemFile(File.createTempFile("quasar", ".json").toString).run.unsafePerformSync,
        openClient = false),
      webConfig)

    (for {
      svc           <- service
      (_, shutdown) <- Http4sUtils.startServers(port, svc).liftM[MainErrT]
      r             <- f(uri).onFinish(_ => shutdown).liftM[MainErrT]
    } yield r).run.unsafePerformSync
  }

  "/mount/fs" should {

    "POST view" in {
      val port = Http4sUtils.anyAvailablePort.unsafePerformSync

      val r = withServer(port, WebConfig.default) { baseUri: Uri =>
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

      val r = withServer(port, WebConfig.default) { baseUri: Uri =>
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

    "MOVE view" in {
      val port = Http4sUtils.anyAvailablePort.unsafePerformSync

      val srcPath = rootDir </> dir("view") </> file("a")
      val dstPath = rootDir </> dir("view") </> file("b")
      val viewConfig = MountConfig.viewConfig(ViewMounterSpec.viewConfig("select * from zips"))

      val webConfig = WebConfig.mountings.set(
        MountingsConfig(Map(srcPath -> viewConfig)))(
        WebConfig.default)

      val r = withServer(port, webConfig) { baseUri: Uri =>
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

    lazy val fileSystemConfigs = {
      val fsCfgs = TestConfig.backendNames
        .traverse((TestConfig.backendEnvName _ >>> TestConfig.loadConfig _)(_).run)
        .map(_
          .unite
          .zipWithIndex
          .map { case (c, i) => (rootDir </> dir("data") </> dir(i.toString)) -> c }
          .toMap[APath, MountConfig])
        .unsafePerformSync

      "fileSystemConfigs empty" <==> (fsCfgs must not be empty)

      fsCfgs
    }

    "MOVE view" in {
      val port = Http4sUtils.anyAvailablePort.unsafePerformSync

      val srcPath = rootDir </> dir("view") </> file("a")
      val dstPath = rootDir </> dir("view") </> file("b")

      val viewConfig = MountConfig.viewConfig(ViewMounterSpec.viewConfig("select 42"))

      val webConfig = WebConfig.mountings.set(
        MountingsConfig(Map(
          srcPath -> viewConfig) ++ fileSystemConfigs))(
        WebConfig.default)

      val r = withServer(port, webConfig) { baseUri: Uri =>
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

    "MOVE a directory containing views and files" in {
      val port = Http4sUtils.anyAvailablePort.unsafePerformSync

      val srcPath = rootDir </> dir("a")
      val dstPath = rootDir </> dir("b")

      val viewConfig = MountConfig.viewConfig(ViewMounterSpec.viewConfig("select 42"))

      val webConfig = WebConfig.mountings.set(
        MountingsConfig(Map(
          (srcPath </> file("view")) -> viewConfig) ++ fileSystemConfigs))(
        WebConfig.default)

      val r = withServer(port, webConfig) { baseUri: Uri =>
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

}

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

package quasar.api.services

import quasar.Predef._
import quasar.{Variables, VariablesArbitrary}
import quasar.api._, ApiErrorEntityDecoder._, PathUtils._
import quasar.api.matchers._
import quasar.contrib.pathy._, PathArbitrary._
import quasar.effect.KeyValueStore
import quasar.fp.liftMT
import quasar.fp.free, free._
import quasar.fs._, InMemory._
import quasar.fs.mount._
import quasar.sql._

import argonaut._, Argonaut._
import matryoshka.data.Fix
import monocle.Lens
import org.http4s._
import org.http4s.argonaut._
import pathy.Path._
import pathy.scalacheck.PathyArbitrary._
import scalaz.{Lens => _, _}
import scalaz.concurrent.Task

object MetadataFixture {

  type MetadataEff[A] = Coproduct[QueryFile, Mounting, A]

  def runQuery(mem: InMemState): QueryFile ~> Task =
    new (QueryFile ~> Task) {
      def apply[A](fs: QueryFile[A]) =
        Task.now(queryFile(fs).eval(mem))
    }

  def runMount(mnts: Map[APath, MountConfig]): Mounting ~> Task =
    new (Mounting ~> Task) {
      type F[A] = State[Map[APath, MountConfig], A]
      val mntr = Mounter.trivial[MountConfigs]
      val kvf = KeyValueStore.impl.toState[F](Lens.id[Map[APath, MountConfig]])
      def apply[A](ma: Mounting[A]) =
        Task.now(mntr(ma).foldMap(kvf).eval(mnts))
    }

  def service(mem: InMemState, mnts: Map[APath, MountConfig]): HttpService =
    metadata.service[MetadataEff].toHttpService(
      liftMT[Task, ResponseT] compose (runQuery(mem) :+: runMount(mnts)))
}

class MetadataServiceSpec extends quasar.Qspec with FileSystemFixture with Http4s {
  import metadata.FsNode
  import VariablesArbitrary._, ExprArbitrary._
  import FileSystemTypeArbitrary._, ConnectionUriArbitrary._
  import MetadataFixture._
  import PathError._

  "Metadata Service" should {
    "respond with NotFound" >> {
      // TODO: escaped paths do not survive being embedded in error messages
      "if directory does not exist" >> prop { dir: ADir => (dir != rootDir) ==> {
        val response = service(InMemState.empty, Map())(Request(uri = pathUri(dir))).unsafePerformSync
        response.as[ApiError].unsafePerformSync must beApiErrorLike(pathNotFound(dir))
      }}

      "file does not exist" >> prop { file: AFile =>
        val response = service(InMemState.empty, Map())(Request(uri = pathUri(file))).unsafePerformSync
        response.as[ApiError].unsafePerformSync must beApiErrorLike(pathNotFound(file))
      }

      "if file with same name as existing directory (without trailing slash)" >> prop { s: SingleFileMemState =>
        depth(s.file) > 1 ==> {
          val parent = fileParent(s.file)
          // .get here is because we know thanks to the property guard, that the parent directory has a name
          val fileWithSameName = parentDir(parent).get </> file(dirName(parent).get.value)
          val response = service(s.state, Map())(Request(uri = pathUri(fileWithSameName))).unsafePerformSync
          response.as[ApiError].unsafePerformSync must beApiErrorLike(pathNotFound(fileWithSameName))
        }
      }
    }

    "respond with OK" >> {
      "and empty list for existing empty directory" >> {
        service(InMemState.empty, Map())(Request(uri = Uri(path = "/")))
          .as[Json].unsafePerformSync must_== Json("children" := List[FsNode]())
      }

      "and list of children for existing nonempty directory" >> prop { s: NonEmptyDir =>
        val childNodes = s.ls.map(FsNode(_, None))

        service(s.state, Map())(Request(uri = pathUri(s.dir)))
          .as[Json].unsafePerformSync must_== Json("children" := childNodes.sorted)
      }.set(minTestsOk = 10)  // NB: this test is slow because NonEmptyDir instances are still relatively large
        .flakyTest("scalacheck: 'Gave up after only 2 passed tests'")

      "and mounts when any children happen to be mount points" >> prop { (
        fName: FileName,
        dName: DirName,
        mName: DirName,
        vName: FileName,
        vcfg: (Fix[Sql], Variables),
        fsCfg: (FileSystemType, ConnectionUri)
      ) => (fName != vName && dName != mName) ==> {
        val parent: ADir = rootDir </> dir("foo")
        val mnts = Map[APath, MountConfig](
          (parent </> file(vName.value), MountConfig.viewConfig(vcfg)),
          (parent </> dir(mName.value), MountConfig.fileSystemConfig(fsCfg)))
        val mem = InMemState fromFiles Map(
          (parent </> file(fName.value), Vector()),
          (parent </> dir(dName.value) </> file("quux"), Vector()),
          (parent </> file(vName.value), Vector()),
          (parent </> dir(mName.value) </> file("bar"), Vector()))

        service(mem, mnts)(Request(uri = pathUri(parent)))
          .as[Json].unsafePerformSync must_=== Json("children" := List(
            FsNode(fName.value, "file", None),
            FsNode(dName.value, "directory", None),
            FsNode(vName.value, "file", Some("view")),
            FsNode(mName.value, "directory", Some(fsCfg._1.value))
          ).sorted)
      }}

      "and empty object for existing file" >> prop { s: SingleFileMemState =>
        service(s.state, Map())(Request(uri = pathUri(s.file)))
          .as[Json].unsafePerformSync must_=== Json.obj()
      }
    }
  }
}

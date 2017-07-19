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

package quasar.api.services

import slamdata.Predef._
import quasar.{Variables, VariablesArbitrary}
import quasar.api._, ApiErrorEntityDecoder._, PathUtils._
import quasar.api.matchers._
import quasar.contrib.pathy._, PathArbitrary._
import quasar.fp._
import quasar.fp.free, free._
import quasar.fs._, InMemory._
import quasar.fs.mount._
import quasar.fs.mount.Fixture.runConstantMount
import quasar.sql._
import quasar.sql.Arbitraries._

import argonaut._, Argonaut._, EncodeJsonScalaz._
import eu.timepit.refined.numeric.{NonNegative, Positive => RPositive}
import eu.timepit.refined.scalacheck.numeric._
import matryoshka.data.Fix
import org.http4s._
import org.http4s.argonaut._
import pathy.Path._
import pathy.scalacheck.PathyArbitrary._
import scalaz.{Lens => _, _}
import scalaz.Scalaz._
import scalaz.concurrent.Task
import shapeless.tag.@@

object MetadataFixture {

  type Eff[A] = Coproduct[QueryFile, Mounting, A]

  def runQuery(mem: InMemState): QueryFile ~> Task =
    queryFile andThen evalNT[Id, InMemState](mem) andThen pointNT[Task]

  def runQueryWithMounts(mem: InMemState, mnts: Map[APath, MountConfig]): QueryFile ~> Task = {
    val run: Eff ~> Task = runQuery(mem) :+: runConstantMount[Task](mnts)
    val addViews = view.queryFile[Eff]
    val addModules = flatMapSNT(transformIn[QueryFile, Eff, Free[Eff, ?]](module.queryFile[Eff], liftFT))
    addViews andThen addModules andThen foldMapNT(run)
  }

  def service(mem: InMemState, mnts: Map[APath, MountConfig]): HttpService = {
    metadata.service[Eff].toHttpService(
      liftMT[Task, ResponseT] compose (runQueryWithMounts(mem, mnts) :+: runConstantMount[Task](mnts)))
  }
}

class MetadataServiceSpec extends quasar.Qspec with FileSystemFixture with Http4s {
  import metadata.FsNode
  import VariablesArbitrary._
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
          .as[Json].unsafePerformSync must_== Json("children" := childNodes.toIList.sorted)
      }.set(minTestsOk = 10)  // NB: this test is slow because NonEmptyDir instances are still relatively large
        .flakyTest("scalacheck: 'Gave up after only 2 passed tests'")

      "and mounts when any children happen to be mount points" >> prop { (
        fileName: FileName,
        directoryName: DirName,
        fsMountName: DirName,
        viewName: FileName,
        moduleName: DirName,
        vcfg: (ScopedExpr[Fix[Sql]], Variables),
        fsCfg: (FileSystemType, ConnectionUri)
      ) => (fileName ≠ viewName &&
          directoryName ≠ fsMountName &&
          directoryName ≠ moduleName &&
          fsMountName ≠ moduleName) ==> {
        val moduleConfig: List[Statement[Fix[Sql]]] = List(
          FunctionDecl(CIName("FOO"), List(CIName("Bar")), Fix(boolLiteral(true))))
        val parent: ADir = rootDir </> dir("foo")
        val mnts = Map[APath, MountConfig](
          (parent </> file1(viewName), MountConfig.viewConfig(vcfg)),
          (parent </> dir1(fsMountName), MountConfig.fileSystemConfig(fsCfg)),
          (parent </> dir1(moduleName), MountConfig.moduleConfig(moduleConfig)))
        val mem = InMemState fromFiles Map(
          (parent </> file1(fileName), Vector()),
          (parent </> dir1(directoryName) </> file("quux"), Vector()),
          (parent </> file1(viewName), Vector()),
          (parent </> dir1(fsMountName) </> file("bar"), Vector()))

        service(mem, mnts)(Request(uri = pathUri(parent)))
          .as[Json].unsafePerformSync must_=== Json("children" := List(
            FsNode(fileName.value, "file", None, None),
            FsNode(directoryName.value, "directory", None, None),
            FsNode(viewName.value, "file", Some("view"), None),
            FsNode(fsMountName.value, "directory", Some(fsCfg._1.value), None),
            FsNode(moduleName.value, "directory", Some("module"), None)
          ).toIList.sorted)
      }}

      "and functions as files on a module mount with additional info about functions parameters" >> prop { dir: ADir =>
        val moduleConfig: List[Statement[Fix[Sql]]] = List(
          FunctionDecl(CIName("FOO"), List(CIName("BAR")), Fix(boolLiteral(true))),
          FunctionDecl(CIName("BAR"), List(CIName("BAR"), CIName("BAZ")), Fix(boolLiteral(false))))
        val mnts = Map[APath, MountConfig](
          (dir, MountConfig.moduleConfig(moduleConfig)))
        val mem = InMemState.empty

        service(mem, mnts)(Request(uri = pathUri(dir)))
          .as[Json].unsafePerformSync must_=== Json("children" := List(
          FsNode("FOO", "file", mount = None, args = Some(List("BAR"))),
          FsNode("BAR", "file", mount = None, args = Some(List("BAR", "BAZ")))
        ).toIList.sorted)
      }

      "support offset and limit" >> prop { (dir: NonEmptyDir, offset: Int @@ NonNegative, limit: Int @@ RPositive) =>
        val childNodes = dir.ls.map(FsNode(_, None))

        service(dir.state, Map())(Request(uri = pathUri(dir.dir).+?("offset", offset.toString).+?("limit", limit.toString)))
          .as[Json].unsafePerformSync must_== Json("children" := childNodes.toIList.sorted.drop(offset).take(limit))
      }.set(minTestsOk = 10) // NB: this test is slow because NonEmptyDir instances are still relatively large

      "and empty object for existing file" >> prop { s: SingleFileMemState =>
        service(s.state, Map())(Request(uri = pathUri(s.file)))
          .as[Json].unsafePerformSync must_=== Json.obj()
      }
    }
  }
}

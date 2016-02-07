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

package quasar.fs

import quasar.Predef._
import quasar.fp._

import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream._

class QueryFilesSpec extends FileSystemTest[FileSystem](FileSystemTest.allFsUT) {
  import FileSystemTest._, FileSystemError._, PathError2._

  val query  = QueryFile.Ops[FileSystem]
  val write  = WriteFile.Ops[FileSystem]
  val manage = ManageFile.Ops[FileSystem]

  val queryPrefix: ADir = rootDir </> dir("forquery")

  def deleteForQuery(run: Run): FsTask[Unit] =
    runT(run)(manage.delete(queryPrefix))

  fileSystemShould { _ => implicit run =>
    "Querying Files" should {
      step(deleteForQuery(run).runVoid)

      "listing directory returns immediate child nodes" >> {
        val d = queryPrefix </> dir("lschildren")
        val d1 = d </> dir("d1")
        val f1 = d1 </> file("f1")
        val f2 = d1 </> dir("d2") </> file("f1")
        val expectedNodes = List[PathName](DirName("d2").left, FileName("f1").right)

        val p = write.save(f1, oneDoc.toProcess).drain ++
                write.save(f2, anotherDoc.toProcess).drain ++
                query.ls(d1).liftM[Process]
                  .flatMap(ns => Process.emitAll(ns.toVector))

        runLogT(run, p)
          .runEither must beRight(containTheSameElementsAs(expectedNodes))
      }

      // TODO: Our chrooting prevents this from working, maybe we need a
      //       spec that does no chrooting and writes no files?
      "listing root dir should succeed" >> todo /* {
        runT(run)(query.ls(rootDir)).runEither must beRight
      } */

      "listing nonexistent directory returns dir NotFound" >> {
        val d = queryPrefix </> dir("lsdne")
        runT(run)(query.ls(d)).runEither must beLeft(pathError(PathNotFound(d)))
      }

      "listing results should not contain deleted files" >> {
        val d = queryPrefix </> dir("lsdeleted")
        val f1 = d </> file("f1")
        val f2 = d </> file("f2")
        val p  = write.save(f1, oneDoc.toProcess).drain ++
                 write.save(f2, anotherDoc.toProcess).drain ++
                 query.ls(d).liftM[Process]
                   .flatMap(ns => Process.emitAll(ns.toVector))

        val preDelete = List[PathName](FileName("f1").right, FileName("f2").right)

        (runLogT(run, p)
          .runEither must beRight(containTheSameElementsAs(preDelete))) and
        (runT(run)(manage.delete(f1) *> query.ls(d))
          .runEither must beRight(containTheSameElementsAs(preDelete.tail)))
      }

      step(deleteForQuery(run).runVoid)
    }; ()
  }
}

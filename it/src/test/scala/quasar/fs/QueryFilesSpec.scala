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

package quasar.fs

import slamdata.Predef._
import quasar.{BackendCapability, Data}
import quasar.Data
import quasar.contrib.pathy._
import quasar.contrib.scalaz.foldable._
import quasar.frontend.logicalplan.{LogicalPlan => LP, _}
import quasar.std.StdLib

import matryoshka.data.Fix
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.stream.Process

class QueryFilesSpec extends FileSystemTest[BackendEffect](FileSystemTest.allFsUT) {
  import FileSystemTest._, FileSystemError._, PathError._, StdLib._

  val query  = QueryFile.Ops[BackendEffect]
  val read   = ReadFile.Ops[BackendEffect]
  val write  = WriteFile.Ops[BackendEffect]
  val manage = ManageFile.Ops[BackendEffect]

  val queryPrefix: ADir = rootDir </> dir("q")

  def deleteForQuery(run: Run): FsTask[Unit] =
    runT(run)(manage.delete(queryPrefix))

  val lpr = new LogicalPlanR[Fix[LP]]

  def readRenamed(src: AFile, from: String, to: String): Fix[LP] =
    lpr.invoke1(
      identity.Squash,
      lpr.invoke2(
        structural.MakeObject,
        lpr.constant(Data._str(to)),
        lpr.invoke2(
          structural.ObjectProject,
          lpr.read(src),
          lpr.constant(Data._str(from)))))

  fileSystemShould { (fs, _) =>
    "Querying Files" should {
      step(deleteForQuery(fs.setupInterpM).runVoid)

      "executing query to an existing file overwrites with results" >> ifSupports(fs,
        BackendCapability.query(),
        BackendCapability.write()) { pendingFor(fs)(Set("mimir")) {

        val d = queryPrefix </> dir("execappends")
        val a = d </> file("afile")
        val b = d </> file("bfile")
        val c = d </> file("out")

        val setup = write.saveThese(a, oneDoc) *> write.saveThese(b, anotherDoc).void

        runT(fs.setupInterpM)(setup).runVoid

        val aToc = readRenamed(a, "a", "c")
        val bToc = readRenamed(b, "b", "c")

        val e = EitherT((query.execute(aToc, c) *> query.execute(bToc, c).void).run.value)
        val p = e.liftM[Process].drain ++ read.scanAll(c)

        runLogT(fs.testInterpM, p).runEither must
          beRight(completelySubsume(Vector[Data](Data.Obj("c" -> Data._int(2)))))
      } }

      "listing directory returns immediate child nodes" >> {
        val d = queryPrefix </> dir("lsch")
        val d1 = d </> dir("d1")
        val f1 = d1 </> file("f1")
        val f2 = d1 </> dir("d2") </> file("f1")
        val expectedNodes = List[PathSegment](DirName("d2").left, FileName("f1").right)

        val setup = write.save(f1, oneDoc.toProcess).drain ++
                    write.save(f2, anotherDoc.toProcess).drain

        execT(fs.setupInterpM, setup).runVoid

        val p = query.ls(d1)

        runT(fs.testInterpM)(p)
          .runEither must beRight(containTheSameElementsAs(expectedNodes))
      }

      // TODO: Our chrooting prevents this from working, maybe we need a
      //       spec that does no chrooting and writes no files?
      "listing root dir should succeed" >> pending {
        runT(fs.testInterpM)(query.ls(rootDir)).runEither must beRight
      }

      "listing nonexistent directory returns dir NotFound" >> pendingFor(fs)(Set("mimir")) {
        val d = queryPrefix </> dir("lsdne")
        runT(fs.testInterpM)(query.ls(d)).runEither must beLeft(pathErr(pathNotFound(d)))
      }

      "listing results should not contain deleted files" >> {
        val d = queryPrefix </> dir("lsdeleted")
        val f1 = d </> file("f1")
        val f2 = d </> file("f2")

        val setup = write.save(f1, oneDoc.toProcess).drain ++
                    write.save(f2, anotherDoc.toProcess).drain
        execT(fs.setupInterpM, setup).runVoid

        val p = query.ls(d)

        val preDelete = List[PathSegment](FileName("f1").right, FileName("f2").right)

        runT(fs.testInterpM)(p)
          .runEither must beRight(containTheSameElementsAs(preDelete))

        runT(fs.setupInterpM)(manage.delete(f1)).runVoid

        runT(fs.testInterpM)(p)
          .runEither must beRight(containTheSameElementsAs(preDelete.tail))
      }

      step(deleteForQuery(fs.setupInterpM).runVoid)
    }
  }
}

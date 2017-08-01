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
import quasar.contrib.pathy._
import quasar.contrib.scalaz.foldable._
import quasar.fs.FileSystemTest.allFsUT

import pathy.Path._
import pathy.scalacheck.PathyArbitrary._
import scalaz._, Scalaz._
import scalaz.stream._

class ManageFilesSpec extends FileSystemTest[BackendEffect](allFsUT.map(_ filter (_.ref supports BackendCapability.write()))) {
  import FileSystemTest._, FileSystemError._, PathError._

  val query  = QueryFile.Ops[BackendEffect]
  val read   = ReadFile.Ops[BackendEffect]
  val write  = WriteFile.Ops[BackendEffect]
  val manage = ManageFile.Ops[BackendEffect]

  val managePrefix: ADir = rootDir </> dir("m")

  def deleteForManage(run: Run): FsTask[Unit] =
    runT(run)(manage.delete(managePrefix))

  fileSystemShould { (fs, _) =>
    implicit val run = fs.testInterpM

    "Managing Files" should {
      step(deleteForManage(fs.setupInterpM).runVoid)

      "moving a file should make it available at the new path and not found at the old" >> {
        val f1 = managePrefix </> dir("d1") </> file("f1")
        val f2 = managePrefix </> dir("d2") </> file("f2")
        val p = write.save(f1, oneDoc.toProcess).drain ++
                manage.moveFile(f1, f2, MoveSemantics.FailIfExists)
                  .liftM[Process].drain ++
                read.scanAll(f2).map(_.left[Boolean]) ++
                (query.fileExistsM(f1))
                  .liftM[Process]
                  .map(_.right[Data])

        runLogT(run, p).map(_.toVector.separate)
          .runEither must beRight((oneDoc, Vector(false)))
      }

      "moving a file to an existing path using FailIfExists semantics should fail with PathExists" >> {
        val f1 = managePrefix </> dir("failifexists") </> file("f1")
        val f2 = managePrefix </> dir("failifexists") </> file("f2")
        val expectedFiles = List[PathSegment](FileName("f1").right, FileName("f2").right)
        val ls = query.ls(managePrefix </> dir("failifexists"))
        val p = write.save(f1, oneDoc.toProcess).drain ++
                write.save(f2, oneDoc.toProcess).drain ++
                manage.moveFile(f1, f2, MoveSemantics.FailIfExists).liftM[Process]

        (execT(run, p).runOption must beSome(pathErr(pathExists(f2)))) and
        (runT(run)(ls).runEither must beRight(containTheSameElementsAs(expectedFiles)))
      }

      "moving a file to an existing path with Overwrite semantics should make contents available at new path" >> {
        val f1 = managePrefix </> dir("overwrite") </> file("f1")
        val f2 = managePrefix </> dir("overwrite") </> file("f2")
        val p = write.save(f1, oneDoc.toProcess).drain ++
                write.save(f2, anotherDoc.toProcess).drain ++
                manage.moveFile(f1, f2, MoveSemantics.Overwrite)
                  .liftM[Process].drain ++
                read.scanAll(f2).map(_.left[Boolean]) ++
                (query.fileExistsM(f1))
                  .liftM[Process]
                  .map(_.right[Data])

        runLogT(run, p).map(_.toVector.separate)
          .runEither must beRight((oneDoc, Vector(false)))
      }

      "moving a file that doesn't exist to a file that does should fail with src NotFound" >> {
        val d = managePrefix </> dir("dnetoexists")
        val f1 = d </> file("f1")
        val f2 = d </> file("f2")
        val expectedFiles = List[PathSegment](FileName("f2").right)
        val ls = query.ls(d)
        val p = write.save(f2, oneDoc.toProcess).drain ++
                manage.moveFile(f1, f2, MoveSemantics.Overwrite)
                  .liftM[Process]

        (execT(run, p).runOption must beSome(pathErr(pathNotFound(f1)))) and
        (runT(run)(ls).runEither must beRight(containTheSameElementsAs(expectedFiles)))
      }

      "moving a file that doesn't exist to a file that also doesn't exist should fail with src NotFound" >> {
        val d = managePrefix </> dir("dnetodne")
        val f1 = d </> file("f1")
        val f2 = d </> file("f2")

        runT(run)(manage.moveFile(f1, f2, MoveSemantics.Overwrite))
          .runOption must beSome(pathErr(pathNotFound(f1)))
      }

      "moving a file to itself with FailIfExists semantics should fail with PathExists" >> {
        val f1 = managePrefix </> dir("selftoself") </> file("f1")
        val p  = write.save(f1, oneDoc.toProcess).drain ++
                 manage.moveFile(f1, f1, MoveSemantics.FailIfExists).liftM[Process]

        execT(run, p).runOption must beSome(pathErr(pathExists(f1)))
      }

      "moving a file to a nonexistent path when using FailIfMissing sematics should fail with dst NotFound" >> {
        val d = managePrefix </> dir("existstodne")
        val f1 = d </> file("f1")
        val f2 = d </> file("f2")
        val expectedFiles = List[PathSegment](FileName("f1").right)
        val p  = write.save(f1, oneDoc.toProcess).drain ++
                 manage.moveFile(f1, f2, MoveSemantics.FailIfMissing).liftM[Process]

        (execT(run, p).runOption must beSome(pathErr(pathNotFound(f2)))) and
        (runT(run)(query.ls(d)).runEither must beRight(containTheSameElementsAs(expectedFiles)))
      }

      "moving a directory should move all files therein to dst path" >> {
        val d = managePrefix </> dir("movedir")
        val d1 = d </> dir("d1")
        val d2 = d </> dir("d2")
        val f1 = d1 </> file("f1")
        val f2 = d1 </> file("f2")

        val expectedFiles = List[PathSegment](FileName("f1").right, FileName("f2").right)

        val p = write.save(f1, oneDoc.toProcess).drain ++
                write.save(f2, anotherDoc.toProcess).drain ++
                manage.moveDir(d1, d2, MoveSemantics.FailIfExists).liftM[Process]

        (execT(run, p).runOption must beNone) and
        (runT(run)(query.ls(d2)).runEither must beRight(containTheSameElementsAs(expectedFiles))) and
        (runT(run)(query.ls(d1)).runEither must beLeft(pathErr(pathNotFound(d1))))
      }

      "moving a nonexistent dir to another nonexistent dir fails with src NotFound" >> {
        val d1 = managePrefix </> dir("dirdnetodirdne") </> dir("d1")
        val d2 = managePrefix </> dir("dirdnetodirdne") </> dir("d2")

        runT(run)(manage.moveDir(d1, d2, MoveSemantics.FailIfExists))
          .runOption must beSome(pathErr(pathNotFound(d1)))
      }

      "[SD-1846] moving a directory with a name that is a prefix of another directory" >> {
        // TODO: folder filenames have been shortened to workaround PostgreSQL table name length restriction — revisit
        val pnt = managePrefix </> dir("SD1846")
        val uf1 = pnt </> dir("UF")   </> file("one")
        val uf2 = pnt </> dir("UF1") </> file("two")
        val uf3 = pnt </> dir("UF2") </> file("three")

        val thirdDoc: Vector[Data] =
          Vector(Data.Obj(ListMap("c" -> Data.Int(1))))

        val src = pnt </> dir("UF")
        val dst = pnt </> dir("UF1") </> dir("UF")

        val setupAndMove =
          write.saveThese(uf1, oneDoc)     *>
          write.saveThese(uf2, anotherDoc) *>
          write.saveThese(uf3, thirdDoc)   *>
          manage.moveDir(src, dst, MoveSemantics.FailIfExists)

        (runT(run)(setupAndMove).runOption must beNone) and
        (runLogT(run, read.scanAll(dst </> file("one"))).runEither must beRight(oneDoc)) and
        (run(query.fileExists(src </> file("one"))).unsafePerformSync must beFalse)
      }

      "deleting a nonexistent file returns PathNotFound" >> {
        val f = managePrefix </> file("delfilenotfound")
        runT(run)(manage.delete(f)).runEither must beLeft(pathErr(pathNotFound(f)))
      }

      "deleting a file makes it no longer accessible" >> {
        val f1 = managePrefix </> dir("deleteone") </> file("f1")
        val p  = write.save(f1, oneDoc.toProcess).drain ++ manage.delete(f1).liftM[Process]

        (execT(run, p).runOption must beNone) and
        (runLogT(run, read.scanAll(f1)).runEither must beRight(Vector.empty[Data]))
      }

      "deleting a file with siblings in directory leaves siblings untouched" >> {
        val d = managePrefix </> dir("withsiblings")
        val f1 = d </> file("f1")
        val f2 = d </> file("f2")

        val p = write.save(f1, oneDoc.toProcess).drain ++
                write.save(f2, anotherDoc.toProcess).drain ++
                manage.delete(f1).liftM[Process]

        (execT(run, p).runOption must beNone)                                       and
        (runLogT(run, read.scanAll(f1)).runEither must beRight(Vector.empty[Data])) and
        (runLogT(run, read.scanAll(f2)).runEither must beRight(anotherDoc))
      }

      "deleting a directory deletes all files therein" >> {
        val d = managePrefix </> dir("deldir")
        val f1 = d </> file("f1")
        val f2 = d </> file("f2")

        val p = write.save(f1, oneDoc.toProcess).drain ++
                write.save(f2, anotherDoc.toProcess).drain ++
                manage.delete(d).liftM[Process]

        (execT(run, p).runOption must beNone)                                       and
        (runLogT(run, read.scanAll(f1)).runEither must beRight(Vector.empty[Data])) and
        (runLogT(run, read.scanAll(f2)).runEither must beRight(Vector.empty[Data])) and
        (runT(run)(query.ls(d)).runEither must beLeft(pathErr(pathNotFound(d))))
      }

      "deleting a nonexistent directory returns PathNotFound" >> {
        val d = managePrefix </> dir("deldirnotfound")
        runT(run)(manage.delete(d)).runEither must beLeft(pathErr(pathNotFound(d)))
      }

      "write/read from temp dir near existing" >> {
        val d = managePrefix </> dir("tmpnear1")
        val f = d </> file("somefile")

        val p = write.save(f, oneDoc.toProcess).drain ++
                manage.tempFile(f).liftM[Process] flatMap { tf =>
                  write.save(tf, anotherDoc.toProcess).drain ++
                  read.scanAll(tf) ++
                  manage.delete(tf).liftM[Process].drain
                }

        runLogT(run, p).runEither must beRight(anotherDoc)
      }

      "write/read from temp dir near non existing" >> {
        val d = managePrefix </> dir("tmpnear2")
        val f = d </> file("somefile")
        val p = manage.tempFile(f).liftM[Process] flatMap { tf =>
                  write.save(tf, anotherDoc.toProcess).drain ++
                  read.scanAll(tf) ++
                  manage.delete(tf).liftM[Process].drain
                }

        runLogT(run, p).runEither must beRight(anotherDoc)
      }

      "temp file should be generated in hint directory" >> prop { rdir: RDir =>
        val hintDir = managePrefix </> rdir

        runT(run)(manage.tempFile(hintDir))
          .map(_ relativeTo hintDir)
          .runEither must beRight(beSome[RFile])
      }

      "temp file should be generated in parent of hint file" >> prop { rfile: RFile =>
        val hintFile = managePrefix </> rfile
        val hintDir  = fileParent(hintFile)

        runT(run)(manage.tempFile(hintFile))
          .map(_ relativeTo hintDir)
          .runEither must beRight(beSome[RFile])
      }

      step(deleteForManage(fs.setupInterpM).runVoid)
    }
  }
}

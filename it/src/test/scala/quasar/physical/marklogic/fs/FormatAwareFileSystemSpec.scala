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

package quasar.physical.marklogic.fs

import slamdata.Predef._
import quasar.Data
import quasar.contrib.pathy._
import quasar.contrib.scalaz.eitherT._
import quasar.fs._

import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

final class FormatAwareFileSystemSpec extends MultiFormatFileSystemTest {
  def dirSeg(name: String): PathSegment  = DirName(name).left
  def fileSeg(name: String): PathSegment = FileName(name).right

  val beAFormatConflictError = beLike[Throwable] {
    case e: Exception => e.getMessage must contain("different format exists")
  }

  def multiFormatFileSystemShould(js: BackendEffect ~> Task, xml: BackendEffect ~> Task) = {
    "FileSystem operations should respect the mount format" >> {
      "creating a file that already exists in a different format should fail" >> {
        val f: AFile = rootDir </> dir("createconflict") </> file("thefile")
        val data     = Vector(Data._int(1), Data._int(2))
        val save     = write.saveThese(f, data)

        val saves  = (runFsE(js)(save) *> runFsE(xml)(save)).run.attempt
        val jsData = read.scanAll(f).translate(runFsE(js)).runLog.run

        (saves |@| jsData)((saveResult, jsd) =>
          (saveResult.toEither must beLeft(beAFormatConflictError)) and
          (jsd.toEither must beRight(containTheSameElementsAs(data)))
        ).unsafePerformSync
      }

      "ls should not show files in other formats" >> {
        val cdir: ADir = rootDir </> dir("lscommon")
        val jsFile = cdir </> file("jsfile")
        val xmlFile = cdir </> file("xmlfile")

        val jsObj: Data  = Data.Obj("js"  -> Data.True)
        val xmlObj: Data = Data.Obj("xml" -> Data.True)

        val saveJs  = write.saveThese(jsFile, Vector(jsObj))
        val saveXml = write.saveThese(xmlFile, Vector(xmlObj))

        (runFsE(xml)(saveXml) *> runFsE(js)(saveJs *> query.ls(cdir)))
          .run.map(_.toEither must beRight(contain(exactly(fileSeg("jsfile")))))
          .unsafePerformSync
      }

      "ls should exclude dirs that only contain files in other formats" >> {
        val cdir: ADir = rootDir </> dir("excludedirs")
        val jsFile = cdir </> file("jsfile")
        val xmlFile = cdir </> dir("xmlfiles") </> file("xmlfile")

        val jsObj: Data  = Data.Obj("js"  -> Data.True)
        val xmlObj: Data = Data.Obj("xml" -> Data.True)

        val saveJs  = write.saveThese(jsFile, Vector(jsObj))
        val saveXml = write.saveThese(xmlFile, Vector(xmlObj))

        (runFsE(xml)(saveXml) *> runFsE(js)(saveJs *> query.ls(cdir)))
          .run.map(_.toEither must beRight(contain(exactly(fileSeg("jsfile")))))
          .unsafePerformSync
      }

      "ls should include dirs contain descendants in current format" >> {
        val cdir: ADir = rootDir </> dir("includedirs")
        val jsFile = cdir </> dir("jsdir1") </> dir("jsdir2") </> file("jsfile")
        val xmlFile = cdir </> dir("xmlfiles") </> file("xmlfile")

        val jsObj: Data  = Data.Obj("js"  -> Data.True)
        val xmlObj: Data = Data.Obj("xml" -> Data.True)

        val saveJs  = write.saveThese(jsFile, Vector(jsObj))
        val saveXml = write.saveThese(xmlFile, Vector(xmlObj))

        (runFsE(xml)(saveXml) *> runFsE(js)(saveJs *> query.ls(cdir)))
          .run.map(_.toEither must beRight(contain(exactly(dirSeg("jsdir1")))))
          .unsafePerformSync
      }

      "move file should fail when dst exists in another format" >> {
        val cdir: ADir = rootDir </> dir("movefails")
        val jsFile = cdir </> file("jsfile")
        val xmlFile = cdir </> file("xmlfile")

        val jsObj: Data  = Data.Obj("js"  -> Data.True)
        val xmlObj: Data = Data.Obj("xml" -> Data.True)

        val saveJs  = write.saveThese(jsFile, Vector(jsObj))
        val saveXml = write.saveThese(xmlFile, Vector(xmlObj))
        val moveJs  = manage.moveFile(jsFile, xmlFile, MoveSemantics.Overwrite)

        val attemptMove = (
          runFsE(xml)(saveXml) *>
          runFsE(js)(saveJs *> moveJs)
        ).run.attempt map (_.toEither must beLeft(beAFormatConflictError))

        val checkExists = (
          runFs(xml)(query.fileExists(xmlFile)) |@|
          runFs(js)(query.fileExists(jsFile))
        )((xmlExists, jsExists) => (xmlExists && jsExists) must beTrue)

        (attemptMove |@| checkExists)(_ and _).unsafePerformSync
      }

      "move dir should move into shared physical dir with other format" >> {
        val cdir: ADir = rootDir </> dir("moveshareddir")
        val jsDir      = cdir </> dir("jsdir")
        val jsA        = jsDir </> file("A")
        val jsB        = jsDir </> file("B")
        val xmlDir     = cdir </> dir("xmldir")
        val xmlC       = xmlDir </> file("C")
        val xmlD       = xmlDir </> file("D")
        val data       = Vector(Data._int(42))

        val writeTpl = (write.saveThese _).tupled
        val saveJs   = List(jsA, jsB).strengthR(data).traverse(writeTpl)
        val saveXml  = List(xmlC, xmlD).strengthR(data).traverse(writeTpl)

        val attemptMove =
          runFsE(xml)(saveXml) *>
          runFsE( js)(saveJs   *> manage.moveDir(jsDir, xmlDir, MoveSemantics.FailIfExists))

        val checkSuccess = (
          runFsE( js)(query.ls( jsDir)).run |@|
          runFsE( js)(query.ls(xmlDir)).run |@|
          runFsE(xml)(query.ls(xmlDir)).run
        ) { (lsjs, jslsxml, lsxml) =>
          (lsjs.toEither    must beLeft)                                                and
          (jslsxml.toEither must beRight(contain(exactly(fileSeg("A"), fileSeg("B"))))) and
          (lsxml.toEither   must beRight(contain(exactly(fileSeg("C"), fileSeg("D")))))
        }

        (attemptMove.run *> checkSuccess).unsafePerformSync
      }

      "move dir should fail when any dst file exists in another format" >> {
        val cdir: ADir = rootDir </> dir("movedirfails")
        val jsDir      = cdir </> dir("jsdir")
        val jsA        = jsDir </> file("A")
        val jsB        = jsDir </> file("B")
        val xmlDir     = cdir </> dir("xmldir")
        val xmlB       = xmlDir </> file("B")
        val xmlC       = xmlDir </> file("C")
        val data       = Vector(Data._int(42))

        val writeTpl = (write.saveThese _).tupled
        val saveJs  = List(jsA, jsB).strengthR(data).traverse(writeTpl)
        val saveXml = List(xmlB, xmlC).strengthR(data).traverse(writeTpl)

        val attemptMove = (
          runFsE(xml)(saveXml) *>
          runFsE(js)(saveJs *> manage.moveDir(jsDir, xmlDir, MoveSemantics.Overwrite))
        ).run.attempt map (_.toEither must beLeft(beAFormatConflictError))

        val ensureNothingChanged = (
          runFsE( js)(query.ls( jsDir)).run |@|
          runFsE(xml)(query.ls(xmlDir)).run
        ) { (lsjs, lsxml) =>
          (lsjs.toEither  must beRight(contain(exactly(fileSeg("A"), fileSeg("B"))))) and
          (lsxml.toEither must beRight(contain(exactly(fileSeg("B"), fileSeg("C")))))
        }

        (attemptMove |@| ensureNothingChanged)(_ and _).unsafePerformSync
      }

      "move dir should not affect files in other formats" >> {
        val cdir: ADir = rootDir </> dir("movefromshared")
        val srcDir     = cdir </> dir("srcdir")
        val jsA        = srcDir </> file("A")
        val jsB        = srcDir </> file("B")
        val xmlC       = srcDir </> file("C")
        val xmlD       = srcDir </> file("D")
        val dstDir     = cdir </> dir("dstdir")
        val data       = Vector(Data._int(42))

        val writeTpl = (write.saveThese _).tupled
        val saveJs   = List(jsA, jsB).strengthR(data).traverse(writeTpl)
        val saveXml  = List(xmlC, xmlD).strengthR(data).traverse(writeTpl)

        val attemptMove =
          runFsE(xml)(saveXml) *>
          runFsE( js)(saveJs   *> manage.moveDir(srcDir, dstDir, MoveSemantics.FailIfExists))

        val checkSuccess = (
          runFsE( js)(query.ls(srcDir)).run |@|
          runFsE( js)(query.ls(dstDir)).run |@|
          runFsE(xml)(query.ls(srcDir)).run
        ) { (jssrc, jsdst, xmlsrc) =>
          (jssrc.toEither  must beLeft)                                                and
          (jsdst.toEither  must beRight(contain(exactly(fileSeg("A"), fileSeg("B"))))) and
          (xmlsrc.toEither must beRight(contain(exactly(fileSeg("C"), fileSeg("D")))))
        }

        (attemptMove.run *> checkSuccess).unsafePerformSync
      }

      "delete dir should not affect files in other formats" >> {
        val cdir: ADir = rootDir </> dir("deleteshared")
        val jsA        = cdir </> file("A")
        val jsB        = cdir </> file("B")
        val xmlC       = cdir </> file("C")
        val xmlD       = cdir </> file("D")
        val data       = Vector(Data._int(42))

        val writeTpl = (write.saveThese _).tupled
        val saveJs   = List(jsA, jsB).strengthR(data).traverse(writeTpl)
        val saveXml  = List(xmlC, xmlD).strengthR(data).traverse(writeTpl)

        val attemptDelete =
          runFsE(xml)(saveXml) *>
          runFsE( js)(saveJs   *> manage.delete(cdir))

        val checkSuccess = (
          runFsE( js)(query.ls(cdir)).run |@|
          runFsE(xml)(query.ls(cdir)).run
        ) { (jsls, xmlls) =>
          (jsls.toEither  must beLeft)                                                and
          (xmlls.toEither must beRight(contain(exactly(fileSeg("C"), fileSeg("D")))))
        }

        (attemptDelete.run *> checkSuccess).unsafePerformSync
      }

      "delete dir should not affect subdirs that still contain files in other formats" >> {
        val cdir: ADir = rootDir </> dir("deletesharedsub")
        val jsA        = cdir   </> file("A")
        val jsB        = cdir   </> file("B")
        val xmlsub     = cdir   </> dir("xmlsub")
        val xmlC       = xmlsub </> file("C")
        val xmlD       = xmlsub </> file("D")
        val data       = Vector(Data._int(42))

        val writeTpl = (write.saveThese _).tupled
        val saveJs   = List(jsA, jsB).strengthR(data).traverse(writeTpl)
        val saveXml  = List(xmlC, xmlD).strengthR(data).traverse(writeTpl)

        val attemptDeleteParent =
          runFsE(xml)(saveXml) *>
          runFsE( js)(saveJs   *> manage.delete(cdir))

        val checkSuccess = (
          runFsE( js)(query.ls(cdir)).run |@|
          runFsE(xml)(query.ls(cdir)).run
        ) { (jsls, xmlls) =>
          (jsls.toEither  must beLeft)                                      and
          (xmlls.toEither must beRight(contain(exactly(dirSeg("xmlsub")))))
        }

        (attemptDeleteParent.run *> checkSuccess).unsafePerformSync
      }
    }
  }
}

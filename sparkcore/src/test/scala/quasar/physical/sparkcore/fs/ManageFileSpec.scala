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

package quasar.physical.sparkcore.fs


import quasar.Predef._
import quasar.QuasarSpecification
import quasar.fp.free._
import quasar.fs.PathError._
import quasar.fs.FileSystemError._
import quasar.fs._
import quasar.fs.ManageFile.MoveSemantics

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}
import java.lang.System

import org.specs2.ScalaCheck
import org.specs2.scalaz._
import pathy.Path._
import scalaz._, Scalaz._, concurrent.Task

class ManageFileSpec extends QuasarSpecification with ScalaCheck with DisjunctionMatchers
    with TempFSSugars {

  type Eff[A] = Task[A]

  "managefile" should {
    "delete" should {

      "delete file on a local FS if exists" in {
        // given
         val program = (path: AFile) => define { unsafe =>
          for {
            _ <- unsafe.delete(path)
          } yield ()
         }
        // when
        withTempFile(createIt = withNoContent) { path =>
          for {
            existed <- exists(path)
            result <- execute(program(path))
            existsAfter <- exists(path)
          } yield {
            // then
            result  must_= \/-(())
            existed must_= true
            existsAfter must_= false
          }
        }
      }

      "fail to delete if file does NOT exist" in {
                // given
         val program = (path: AFile) => define { unsafe =>
          for {
            _ <- unsafe.delete(path)
          } yield ()
         }
        // when
        withTempFile(createIt = None) { path =>
          for {
            existed <- exists(path)
            result <- execute(program(path))
          } yield {
            // then
            existed must_= false
            result must_= -\/((PathErr(PathNotFound(path))))
          }
        }
      }

      "fail to delete if folder not empty" in {
        // given
        val program = (path: APath) => define { unsafe =>
          for {
            _ <- unsafe.delete(path)
          } yield ()
        }

        withTempDir(createIt = true) { (dirPath: ADir) =>
          for {
            filePath <- createFile(dirPath, "temp.tmp")
            result <- execute(program(dirPath))
          } yield {
            result must_= -\/((PathErr(PathNotFound(dirPath))))
          }
        }
      }
    }

    "tempFile" should {
      "create temp file near existing path" in {
        // given
        val program = (nearDir: ADir) => define { unsafe =>
          for {
            filePath <- unsafe.tempFile(nearDir)
          } yield (filePath)
        }
        // when
        withTempDir(createIt = true) { nearDir =>
          for {
            result <- execute(program(nearDir))
          } yield {
            // then
            result must beRightDisjunction.like {
              case path =>
                posixCodec.unsafePrintPath(path)
                  .startsWith(posixCodec.unsafePrintPath(nearDir)) must_= true
            }
          }
        }
      }
      
      "fail if near is a non existing directory" in {
        // given
        val nearDir = rootDir </> dir("nonexisting")
        val program = define { unsafe =>
          for {
            filePath <- unsafe.tempFile(nearDir)
          } yield (filePath)
        }
        // when
        val result = execute(program).unsafePerformSync
        // then
        result must_= -\/((PathErr(PathNotFound(nearDir))))
        ok
      }
      
      "fail if near is a file (not a directory)" in {
        // given
        val program = (nearDir: APath) => define { unsafe =>
          for {
            filePath <- unsafe.tempFile(nearDir)
          } yield (filePath)
        }
        // when
        withTempFile(createIt = withNoContent) { filePath =>
          for {
            result <- execute(program(filePath))
          } yield {
            // then
            result must_= -\/((PathErr(
              InvalidPath(filePath, s"Provided $filePath is not a directory"))))
          }
        }
      }
    }

    "move" should {

      "move file from source to desination when semantics == overwrite & dst does not exist" in {
        // given
        val program = (src: AFile, dst: AFile) => define { unsafe =>
          for {
            filePath <- unsafe.moveFile(src, dst, MoveSemantics.Overwrite)
          } yield (filePath)
        }
        // then
        withTempFile(createIt = Some(List("some content"))) { src =>
          Task.delay {
            withTempFile(createIt = None) { dst =>
              for {
                result <- execute(program(src, dst))
                srcExists <- exists(src)
                dstExists <- exists(dst)
                dstContent <- getContent(dst)
              } yield {
                // then
                srcExists must_= false
                dstExists must_= true
                dstContent must_= List("some content")
              }
            }
          }
        }
      }

      "move file from source to desination when semantics == overwrite & dst does exist" in {
        // given
        val program = (src: AFile, dst: AFile) => define { unsafe =>
          for {
            filePath <- unsafe.moveFile(src, dst, MoveSemantics.Overwrite)
          } yield (filePath)
        }
        // then
        withTempFile(createIt = Some(List("src content"))) { src =>
          Task.delay {
            withTempFile(createIt = Some(List("dst content"))) { dst =>
              for {
                result <- execute(program(src, dst))
                srcExists <- exists(src)
                dstExists <- exists(dst)
                dstContent <- getContent(dst)
              } yield {
                // then
                srcExists must_= false
                dstExists must_= true
                dstContent must_= List("src content")
              }
            }
          }
        }
      }

      "move file from src to dst when semantics == failIfExists & dst does not exist" in {
        // given
        val program = (src: AFile, dst: AFile) => define { unsafe =>
          for {
            filePath <- unsafe.moveFile(src, dst, MoveSemantics.FailIfExists)
          } yield (filePath)
        }
        // then
        withTempFile(createIt = Some(List("some content"))) { src =>
          Task.delay {
            withTempFile(createIt = None) { dst =>
              for {
                result <- execute(program(src, dst))
                srcExists <- exists(src)
                dstExists <- exists(dst)
                dstContent <- getContent(dst)
              } yield {
                // then
                srcExists must_= false
                dstExists must_= true
                dstContent must_= List("some content")
              }
            }
          }
        }
      }

      "fail when try yo move file from src to dst when semantics == failIfExists & dst does exist" in {
        // given
        val program = (src: AFile, dst: AFile) => define { unsafe =>
          for {
            filePath <- unsafe.moveFile(src, dst, MoveSemantics.FailIfExists)
          } yield (filePath)
        }
        // then
        withTempFile(createIt = Some(List("src content"))) { src =>
          Task.delay {
            withTempFile(createIt = Some(List("dst content"))) { dst =>
              for {
                result <- execute(program(src, dst))
                srcExists <- exists(src)
                dstExists <- exists(dst)
                srcContent <- getContent(src)
                dstContent <- getContent(dst)
              } yield {
                // then
                result must_= -\/((PathErr(
                  InvalidPath(dst,
                    "Can not move to destination that already exists if semnatics == failIfExists")))
                )
                srcExists must_= true
                dstExists must_= true
                srcContent must_= List("src content")
                dstContent must_= List("dst content")
              }
            }
          }
        }
      }

      "move file from src to dst when semantics == failIfMissing & dst does exist" in {
        // given
        val program = (src: AFile, dst: AFile) => define { unsafe =>
          for {
            filePath <- unsafe.moveFile(src, dst, MoveSemantics.FailIfMissing)
          } yield (filePath)
        }
        // then
        withTempFile(createIt = Some(List("src content"))) { src =>
          Task.delay {
            withTempFile(createIt = Some(List("dst content"))) { dst =>
              for {
                result <- execute(program(src, dst))
                srcExists <- exists(src)
                dstExists <- exists(dst)
                dstContent <- getContent(dst)
              } yield {
                // then
                srcExists must_= false
                dstExists must_= true
                dstContent must_= List("src content")
              }
            }
          }
        }
      }

      "fail when try to move file from src to dst when semantics == failIfMissing & dst does not exist" in {
        // given
        val program = (src: AFile, dst: AFile) => define { unsafe =>
          for {
            filePath <- unsafe.moveFile(src, dst, MoveSemantics.FailIfMissing)
          } yield (filePath)
        }
        // then
        withTempFile(createIt = Some(List("src content"))) { src =>
          Task.delay {
            withTempFile(createIt = None) { dst =>
              for {
                result <- execute(program(src, dst))
                srcExists <- exists(src)
                dstExists <- exists(dst)

              } yield {
                // then
                result must_= -\/((PathErr(
                  InvalidPath(dst,
                    "Can not move to destination that does not exists if semnatics == failIfMissing")))
                )
                srcExists must_= true
              }
            }
          }
        }
      }

      "move directory from src to dst when semantics == overwrite & dst does not exist" in {
        // given
        val program = (src: ADir, dst: ADir) => define { unsafe =>
          for {
            filePath <- unsafe.moveDir(src, dst, MoveSemantics.Overwrite)
          } yield (filePath)
        }
        withTempDir(createIt = true) { src =>
          Task.delay {
            withTempDir(createIt = false) { dst =>
              for {
                _ <- createFile(src, "temp1.tmp")
                _ <- createFile(src, "temp2.tmp")
                result <- execute(program(src, dst))
                srcExists <- exists(src)
                dstExists <- exists(dst)
                children <- getChildren(dst)
              } yield {
                srcExists must_= false
                dstExists must_= true
                children must_= List(
                  dst </> file("temp1.tmp"),
                  dst </> file("temp2.tmp")
                )
              }
            }
          }
        }
      }

      "move directory from src to dst when semantics == overwrite & dst does exist" in {
        // given
        val program = (src: ADir, dst: ADir) => define { unsafe =>
          for {
            filePath <- unsafe.moveDir(src, dst, MoveSemantics.Overwrite)
          } yield (filePath)
        }
        withTempDir(createIt = true) { src =>
          Task.delay {
            withTempDir(createIt = true) { dst =>
              for {
                _ <- createFile(src, "temp1.tmp")
                _ <- createFile(src, "temp2.tmp")
                _ <- createFile(dst, "temp3.tmp")
                _ <- createFile(dst, "temp4.tmp")
                result <- execute(program(src, dst))
                srcExists <- exists(src)
                dstExists <- exists(dst)
                children <- getChildren(dst)
              } yield {
                srcExists must_= false
                dstExists must_= true
                children must_= List(
                  dst </> file("temp1.tmp"),
                  dst </> file("temp2.tmp")
                )
              }
            }
          }
        }
      }

      "move directory from src to dst when semantics == failIfExists & dst does not exist" in {
        // given
        val program = (src: ADir, dst: ADir) => define { unsafe =>
          for {
            filePath <- unsafe.moveDir(src, dst, MoveSemantics.FailIfExists)
          } yield (filePath)
        }
        withTempDir(createIt = true) { src =>
          Task.delay {
            withTempDir(createIt = false) { dst =>
              for {
                _ <- createFile(src, "temp1.tmp")
                _ <- createFile(src, "temp2.tmp")
                result <- execute(program(src, dst))
                srcExists <- exists(src)
                dstExists <- exists(dst)
                children <- getChildren(dst)
              } yield {
                srcExists must_= false
                dstExists must_= true
                children must_= List(
                  dst </> file("temp1.tmp"),
                  dst </> file("temp2.tmp")
                )
              }
            }
          }
        }
      }

      "fail when try to move dir from src to dst when semantics == failIfExists & dst does exist" in {
        // given
        val program = (src: ADir, dst: ADir) => define { unsafe =>
          for {
            filePath <- unsafe.moveDir(src, dst, MoveSemantics.FailIfExists)
          } yield (filePath)
        }
        // when
        withTempDir(createIt = true) { src =>
          Task.delay {
            withTempDir(createIt = true) { dst =>
              for {
                _ <- createFile(src, "temp1.tmp")
                _ <- createFile(src, "temp2.tmp")
                _ <- createFile(dst, "temp3.tmp")
                _ <- createFile(dst, "temp4.tmp")
                result <- execute(program(src, dst))
              } yield {
                // then
                result must_= -\/((PathErr(
                  InvalidPath(dst,
                    "Can not move to destination that already exists if semnatics == failIfExists")))
                )
              }
            }
          }
        }

      }

      "fail when try to move dir from src to dest when semantics == failIfMissing & dst does not exist" in {
        // given
        val program = (src: ADir, dst: ADir) => define { unsafe =>
          for {
            filePath <- unsafe.moveDir(src, dst, MoveSemantics.FailIfMissing)
          } yield (filePath)
        }
        withTempDir(createIt = true) { src =>
          Task.delay {
            withTempDir(createIt = false) { dst =>
              for {
                _ <- createFile(src, "temp1.tmp")
                _ <- createFile(src, "temp2.tmp")
                result <- execute(program(src, dst))
              } yield {
                // then
                result must_= -\/((PathErr(
                  InvalidPath(dst,
                    "Can not move to destination that does not exists if semnatics == failIfMissing")))
                )
              }
            }
          }
        }
      }

      "move directory from src to dst when semantics == failIfMissing & dst does exist" in {
        // given
        val program = (src: ADir, dst: ADir) => define { unsafe =>
          for {
            filePath <- unsafe.moveDir(src, dst, MoveSemantics.FailIfMissing)
          } yield (filePath)
        }
        // when
        withTempDir(createIt = true) { src =>
          Task.delay {
            withTempDir(createIt = true) { dst =>
              for {
                _ <- createFile(src, "temp1.tmp")
                _ <- createFile(src, "temp2.tmp")
                _ <- createFile(dst, "temp3.tmp")
                _ <- createFile(dst, "temp4.tmp")
                result <- execute(program(src, dst))
                srcExists <- exists(src)
                dstExists <- exists(dst)
                children <- getChildren(dst)
              } yield {
                // then
                srcExists must_= false
                dstExists must_= true
                children must_= List(
                  dst </> file("temp1.tmp"),
                  dst </> file("temp2.tmp")
                )
              }
            }
          }
        }
      }
    }
  }

  private def execute[C](program: FileSystemErrT[Free[ManageFile, ?], C]):
      Task[FileSystemError \/ C] =
    program.run.foldMap(interpreter)

  private def interpreter: ManageFile ~> Task =
    local.managefile.interpret[Eff] andThen foldMapNT(NaturalTransformation.refl[Task])

  private def tempFile(createFile: Boolean = true, maybeContent: Option[String] = None): AFile = {
    val pathStr = s"""${System.getProperty("java.io.tmpdir")}/${scala.util.Random.nextInt().toString}.tmp"""
    val extensin = ".tmp"

    if(createFile) {
      val file = new File(pathStr)
      file.createNewFile()

      maybeContent.foreach { content =>
        val pw = new PrintWriter(file)
        pw.write(content)
        pw.flush
        pw.close
      }
    }

    sandboxAbs(posixCodec.parseAbsFile(pathStr).get)
  }

  private def tempDir(createDir: Boolean = true, withFiles: List[String] = List()): ADir = {
    val root = Paths.get(System.getProperty("java.io.tmpdir"))
    val prefix = "tempDir"
    val dirName = prefix + scala.util.Random.nextInt().toString
    val path = s"$root/$dirName/"

    if(createDir) {
      Files.createDirectory(Paths.get(path))
      withFiles.foreach { fileName =>
        new File(path + fileName).createNewFile()
      }
    }
    sandboxAbs(posixCodec.parseAbsDir(path).get)
  }


  private def define[C]
    (defined: ManageFile.Ops[ManageFile] => FileSystemErrT[Free[ManageFile, ?], C])
    (implicit ops: ManageFile.Ops[ManageFile])
      : FileSystemErrT[Free[ManageFile, ?], C] =
    defined(ops)

}

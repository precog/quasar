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
import quasar.fs._

import java.lang.System
import java.nio.file._
import java.io.{PrintWriter, File}

import pathy.Path.posixCodec
import pathy.Path._
import scalaz.concurrent.Task


trait TempFSSugars {

  def exists(path: APath): Task[Boolean] = Task.delay {
    Files.exists(Paths.get(posixCodec.unsafePrintPath(path)))
  }

  def getContent(f: AFile): Task[List[String]] =
    Task.delay(scala.io.Source.fromFile(posixCodec.unsafePrintPath(f)).getLines.toList)

  def withTempDir[C](createIt: Boolean = true, withTailDir: List[String] = Nil)
    (run: ADir => Task[C]): C = {

    def genDirPath: Task[ADir] = Task.delay {
      val root = System.getProperty("java.io.tmpdir")
      val prefix = "tempDir"
      val tailStr = withTailDir.mkString("/") + "/"
      val random = scala.util.Random.nextInt().toString
      val path = s"$root/$prefix-$random/$tailStr"
      sandboxAbs(posixCodec.parseAbsDir(path).get)
    }

    def createDir(dirPath: ADir): Task[Unit] = Task.delay {
      if(createIt) {
        Files.createDirectory(Paths.get(posixCodec.unsafePrintPath(dirPath)))
        ()
      } else ()
    }

    def deleteDir(dirPath: ADir): Task[Unit] = for {
      root <- Task.delay{ System.getProperty("java.io.tmpdir") }
      _ <- {
        if(parseDir(root) == dirPath) {
          Task.now(())
        } else {
          toNioPath(dirPath).toFile.listFiles().foreach(_.delete())
          Files.delete(toNioPath(dirPath))
          parentDir(dirPath).fold(Task.now(()))(p => deleteDir(p))
        }
      }
    } yield ()

    (for {
      dirPath <- genDirPath
      _  <- createDir(dirPath)
      result <- run(dirPath).onFinish {
        _ => deleteDir(dirPath)
      }
    } yield result).unsafePerformSync
  }

  def createFile(in: ADir, name: String) = Task.delay {
    val path = in </> file(name)
    toNioPath(path).toFile().createNewFile()
  }

  type Content = List[String]

  def withNoContent: Option[Content] = Some(List.empty[String])

  def withTempFile[C](createIt: Option[Content] = None)(run: AFile => Task[C]): C = {

    def genTempFilePath: Task[AFile] = Task.delay {
      val path = System.getProperty("java.io.tmpdir") +
      "/" + scala.util.Random.nextInt().toString + ".tmp"
      sandboxAbs(posixCodec.parseAbsFile(path).get)
    }

    def createFile(filePath: AFile): Task[Unit] = Task.delay {
      createIt.foreach { content =>
        val file = toNioPath(filePath).toFile()
        val writer = new PrintWriter(file)
        content.foreach {
          line => writer.write(line + "\n")
        }
        writer.flush()
        writer.close()
      }
    }

    def deleteFile(file: AFile): Task[Unit] = Task.delay {
      Files.delete(Paths.get(posixCodec.unsafePrintPath(file)))
    }

    val execution: Task[C] = for {
      filePath <- genTempFilePath
      _ <- createFile(filePath)
      result <- run(filePath).onFinish {
        _ => deleteFile(filePath)
      }
    } yield result

    execution.unsafePerformSync
  }

  def getChildren(dir: ADir): Task[List[AFile]] = Task.delay {
    val dirFile = new File(posixCodec.unsafePrintPath(dir))
    dirFile.listFiles.toList.map(f => sandboxAbs(posixCodec.parseAbsFile(f.getAbsolutePath).get))
  }


  def toNioPath(path: APath) =
    Paths.get(posixCodec.unsafePrintPath(path))

  def parseDir(dirStr: String): ADir =
    sandboxAbs(posixCodec.parseAbsDir(dirStr).get)

}

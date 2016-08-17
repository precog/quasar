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
import quasar.Data
import quasar.fp.TaskRef
import quasar.fp.numeric._
import quasar.fp.free._
import quasar.fs._
import quasar.fs.WriteFile.WriteHandle
import quasar.effect._

import java.io._

import pathy.Path._
import org.specs2.ScalaCheck
import scalaz._, Scalaz._, concurrent.Task

class WriteFileSpec extends QuasarSpecification with ScalaCheck with TempFSSugars  {

  type Eff0[A] = Coproduct[KeyValueStore[WriteHandle, PrintWriter, ?], Task, A]
  type Eff[A] = Coproduct[MonotonicSeq, Eff0, A]

  "writefile" should {

    "open -> write chunks -> close -> assert file with results" in {
      // given
      val chunks = (user("john", 31) :: user("anna", 21) :: Nil).toVector
      val program = (path: AFile) => define { writeUnsafe =>
        for {
          wh <- writeUnsafe.open(path)
          errors <- writeUnsafe.write(wh, chunks).liftM[FileSystemErrT]
          _ <- writeUnsafe.close(wh).liftM[FileSystemErrT]
        } yield (errors)
      }
      // when
      withTempFile(createIt = None) { path =>
        for {
          result <- execute[Vector[FileSystemError]](program(path))
          content <- getContent(path)
        } yield{
          // then
          result must_= \/-(Vector.empty[FileSystemError])
          content  must_= List(
            """{ "login": "john", "age": 31 }""",
            """{ "login": "anna", "age": 21 }"""
          )
        }
      }
    }

    "(open -> write chunks -> close) -> (open -> write chunks -> close) -> assert file with results" in {
      // given
      val chunks1 = (user("john", 31) :: user("anna", 21) :: Nil).toVector
      val chunks2 = (user("kate", 22) :: Nil).toVector

      val program = (path: AFile) => define { writeUnsafe =>
        for {
          wh1 <- writeUnsafe.open(path)
          errors1 <- writeUnsafe.write(wh1, chunks1).liftM[FileSystemErrT]
          _ <- writeUnsafe.close(wh1).liftM[FileSystemErrT]
          wh2 <- writeUnsafe.open(path)
          errors2 <- writeUnsafe.write(wh2, chunks2).liftM[FileSystemErrT]
          _ <- writeUnsafe.close(wh2).liftM[FileSystemErrT]

        } yield (errors1 ++ errors2)
      }
      // when
      withTempFile() { path =>
        for {
          result <- execute[Vector[FileSystemError]](program(path))
          content <- getContent(path)
        } yield {
          // then
          result must_= \/-(Vector.empty[FileSystemError])
          content must_= List(
            """{ "login": "john", "age": 31 }""",
            """{ "login": "anna", "age": 21 }""",
            """{ "login": "kate", "age": 22 }"""
          )
        }
      }
    }

    "should create dir if parent dir and file does not exist" in {
      // given
      val program = (path: AFile) => define { unsafe =>
        for {
          wh <- unsafe.open(path)
          _ <- unsafe.close(wh).liftM[FileSystemErrT]
        } yield ()
      }
      // when
      withTempDir(createIt = false) { dirPath =>
        for {
          dirExisted <- exists(dirPath)
          filePath = dirPath </> file("some_file.tmp")
          result <- execute[Unit](program(filePath))
          dirExists <- exists(dirPath)
          fileExists <- exists(filePath)
        } yield {
          // then
          dirExisted must_= false
          dirExists must_= true
          fileExists must_= true
        }
      }
    }

    "should create dirs if parent dirs and file does not exist" in {
      // given
      val program = (path: AFile) => define { unsafe =>
        for {
          wh <- unsafe.open(path)
          _ <- unsafe.close(wh).liftM[FileSystemErrT]
        } yield ()
      }
      // when
      withTempDir(createIt = false, withTailDir = List("foo", "bar")) { dirPath =>
          for {
            dirsExisted <- exists(dirPath)
            filePath = dirPath </> file("some_file.tmp")
            result <- execute[Unit](program(filePath))
            dirsExists <- exists(dirPath)
            fileExists <- exists(filePath)
          } yield {
            // then
            dirsExisted must_= false
            dirsExists must_= true
            fileExists must_= true
          }
      }
    }
  }
  
  private def execute[C](program: FileSystemErrT[Free[WriteFile, ?], C]):
      Task[FileSystemError \/ C] = interpreter.flatMap(program.run.foldMap(_))
 
  private def interpreter: Task[WriteFile ~> Task] = {

    def innerInterpreter: Task[Eff ~> Task] =  {
      (TaskRef(0L) |@| TaskRef(Map.empty[WriteHandle, PrintWriter])) { (genState, kvsState) =>
        MonotonicSeq.fromTaskRef(genState) :+:
        KeyValueStore.fromTaskRef[WriteHandle, PrintWriter](kvsState) :+:
        NaturalTransformation.refl[Task]
      }
    }

    innerInterpreter.map { inner =>
      local.writefile.interpret[Eff] andThen foldMapNT[Eff, Task](inner)
    }
  }
  
  private def define[C]
    (defined: WriteFile.Unsafe[WriteFile] => FileSystemErrT[Free[WriteFile, ?], C])
    (implicit writeUnsafe: WriteFile.Unsafe[WriteFile])
      : FileSystemErrT[Free[WriteFile, ?], C] =
    defined(writeUnsafe)
  
  private def user(login: String, age: Int) =
    Data.Obj(ListMap("login" -> Data.Str(login), "age" -> Data.Int(age)))
}

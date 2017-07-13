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

package quasar.yggdrasil.vfs

import quasar.precog.util.IOUtils

import fs2.Stream
import fs2.interop.scalaz._

import org.specs2.mutable._

import pathy.Path

import scalaz.{~>, Coproduct, NaturalTransformation}
import scalaz.concurrent.Task

import scodec.bits.ByteVector

import java.nio.file.Files
import java.io.{File, FileInputStream, FileOutputStream}

object RealPOSIXSpecs extends Specification {
  import POSIXOp._

  "real posix interpreter" should {
    "mkdir on root if non-existent" in {
      val target = new File(newBase(), "non-existent")
      RealPOSIX(target).unsafePerformSync

      target.exists() mustEqual true
      target.isDirectory() mustEqual true
    }

    "produce distinct UUIDs" in setupDir { base =>
      val test = for {
        interp <- RealPOSIX(base)
        first <- interp(GenUUID)
        second <- interp(GenUUID)
      } yield (first, second)

      val (first, second) = test.unsafePerformSync

      first must not(be(second))
    }

    "open a file and return its contents" in setupDir { base =>
      val file = new File(base, "test")

      val fos = new FileOutputStream(file)
      fos.write(Array[Byte](1, 2, 3, 4, 5))
      fos.close()

      val test = for {
        interp <- RealPOSIX(base)
        results <- interp(OpenR(Path.rootDir </> Path.file("test")))
        contents <- translate(results, interp).fold(ByteVector.empty)(_ ++ _).runLast
      } yield contents

      test.unsafePerformSync must beSome(ByteVector(1, 2, 3, 4, 5))
    }

    "write to a file" in setupDir { base =>
      val file = new File(base, "test")

      val test = for {
        interp <- RealPOSIX(base)
        sink <- interp(OpenW(Path.rootDir </> Path.file("test")))
        driver = Stream(ByteVector(1, 2, 3, 4, 5)).to(sink)
        _ <- translate(driver, interp).run
      } yield ()

      test.unsafePerformSync

      val fis = new FileInputStream(file)
      val buffer = new Array[Byte](10)
      fis.read(buffer) mustEqual 5
      fis.close()

      buffer mustEqual Array(1, 2, 3, 4, 5, 0, 0, 0, 0, 0)
    }

    "list the contents of a directory" in setupDir { base =>
      val targets = List("test1", "test2", "test3")
      targets.foreach(target => Files.createFile(new File(base, target).toPath()))

      val test = for {
        interp <- RealPOSIX(base)
        files <- interp(Ls(Path.rootDir))
      } yield files

      test.unsafePerformSync must containTheSameElementsAs(targets.map(Path.file))
    }

    "make a subdirectory" in setupDir { base =>
      val target = "foo"

      val test = for {
        interp <- RealPOSIX(base)
        _ <- interp(MkDir(Path.rootDir </> Path.dir(target)))
      } yield ()

      test.unsafePerformSync

      val ftarget = new File(base, target)

      ftarget.exists() mustEqual true
      ftarget.isDirectory() mustEqual true
    }

    "link two directories" in setupDir { base =>
      val from = "foo"
      val to = "bar"

      val test = for {
        interp <- RealPOSIX(base)
        _ <- interp(MkDir(Path.rootDir </> Path.dir(from)))
        _ <- interp(LinkDir(Path.rootDir </> Path.dir(from), Path.rootDir </> Path.dir(to)))
      } yield ()

      test.unsafePerformSync

      val ffrom = new File(base, from)
      val fto = new File(base, to)

      fto.exists() mustEqual true
      fto.isDirectory() mustEqual true

      Files.createFile(new File(ffrom, "test").toPath())

      new File(fto, "test").exists() mustEqual true
    }

    "link two files" in setupDir { base =>
      val from = "foo"
      val to = "bar"

      val test = for {
        interp <- RealPOSIX(base)

        sink1 <- interp(OpenW(Path.rootDir </> Path.file(from)))
        driver1 = Stream(ByteVector(1, 2, 3)).to(sink1)
        _ <- translate(driver1, interp).run

        _ <- interp(LinkFile(Path.rootDir </> Path.file(from), Path.rootDir </> Path.file(to)))

        sink2 <- interp(OpenW(Path.rootDir </> Path.file(from)))
        driver2 = Stream(ByteVector(4, 5, 6)).to(sink2)
        _ <- translate(driver2, interp).run
      } yield ()

      test.unsafePerformSync

      val fis = new FileInputStream(new File(base, to))
      val buffer = new Array[Byte](3)
      fis.read(buffer) mustEqual 3
      fis.close()

      buffer mustEqual Array(4, 5, 6)
    }

    "move a file" in setupDir { base =>
      val from = "foo"
      val to = "bar"

      Files.createFile(new File(base, from).toPath())

      val test = for {
        interp <- RealPOSIX(base)
        _ <- interp(Move(Path.rootDir </> Path.file(from), Path.rootDir </> Path.file(to)))
      } yield ()

      test.unsafePerformSync

      new File(base, from).exists() mustEqual false
      new File(base, to).exists() mustEqual true
    }

    "check if a file exists" in setupDir { base =>
      val foo = "foo"
      val bar = "bar"

      Files.createFile(new File(base, foo).toPath())

      val test = for {
        interp <- RealPOSIX(base)
        fooEx <- interp(Exists(Path.rootDir </> Path.file(foo)))
        barEx <- interp(Exists(Path.rootDir </> Path.file(bar)))
      } yield (fooEx, barEx)

      val (fooEx, barEx) = test.unsafePerformSync

      fooEx mustEqual true
      barEx mustEqual false
    }

    "check if a directory exists" in setupDir { base =>
      val foo = "foo"
      val bar = "bar"

      new File(base, foo).mkdir()

      val test = for {
        interp <- RealPOSIX(base)
        fooEx <- interp(Exists(Path.rootDir </> Path.dir(foo)))
        barEx <- interp(Exists(Path.rootDir </> Path.dir(bar)))
      } yield (fooEx, barEx)

      val (fooEx, barEx) = test.unsafePerformSync

      fooEx mustEqual true
      barEx mustEqual false
    }

    "delete a file" in setupDir { base =>
      val target = "foo"

      Files.createFile(new File(base, target).toPath())

      val test = for {
        interp <- RealPOSIX(base)
        _ <- interp(Delete(Path.rootDir </> Path.file(target)))
      } yield ()

      test.unsafePerformSync

      new File(base, target).exists() mustEqual false
    }

    "delete an empty directory" in setupDir { base =>
      val target = "foo"
      val ftarget = new File(base, target)

      ftarget.mkdir()

      val test = for {
        interp <- RealPOSIX(base)
        _ <- interp(Delete(Path.rootDir </> Path.dir(target)))
      } yield ()

      test.unsafePerformSync

      ftarget.exists() mustEqual false
    }

    "delete a non-empty directory" in setupDir { base =>
      val target = "foo"
      val ftarget = new File(base, target)

      ftarget.mkdir()

      Files.createFile(new File(ftarget, "bar").toPath())

      val test = for {
        interp <- RealPOSIX(base)
        _ <- interp(Delete(Path.rootDir </> Path.dir(target)))
      } yield ()

      test.unsafePerformSync

      ftarget.exists() mustEqual false
    }

    "delete a non-empty dir symlink" in setupDir { base =>
      val target = "foo"
      val ftarget = new File(base, target)

      ftarget.mkdir()

      Files.createFile(new File(ftarget, "test").toPath())

      val test = for {
        interp <- RealPOSIX(base)
        _ <- interp(LinkDir(Path.rootDir </> Path.dir("foo"), Path.rootDir </> Path.dir("bar")))
        _ <- interp(Delete(Path.rootDir </> Path.dir("bar")))
      } yield ()

      test.unsafePerformSync

      ftarget.exists() mustEqual true
      new File(ftarget, "test").exists() mustEqual true

      new File(base, "bar").exists() mustEqual false
    }
  }

  def translate[A](str: Stream[POSIXWithTask, A], interp: POSIXOp ~> Task): Stream[Task, A] = {
    import fs2.util.UF1

    val nt = λ[UF1[POSIXWithTask, Task]] { pwt =>
      val fullInt =
        λ[Coproduct[POSIXOp, Task, ?] ~> Task](_.fold(interp, NaturalTransformation.refl[Task]))

      pwt.foldMap(fullInt)
    }

    str.translate(nt)
  }

  def setupDir[A](body: File => A): A = {
    val base = newBase()
    val back = body(base)
    IOUtils.recursiveDelete(base).unsafePerformIO()
    back
  }

  def newBase() = Files.createTempDirectory("RealPOSIXSpecs").toFile
}

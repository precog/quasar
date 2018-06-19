/*
 * Copyright 2014–2018 SlamData Inc.
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

import cats.arrow.FunctionK
import cats.effect.IO

import fs2.Stream

import org.specs2.mutable._

import pathy.Path

import scalaz.{~>, NaturalTransformation}

import shims._

import scodec.bits.ByteVector

import java.nio.file.Files
import java.io.{File, FileInputStream, FileOutputStream}

import iotaz.CopK

object RealPOSIXSpecs extends Specification {
  import POSIXOp._

  def ioPOSIX(root: File): IO[POSIXOp ~> IO] =
    RealPOSIX[IO](root)

  "real posix interpreter" should {
    "mkdir on root if non-existent" in {
      val target = new File(newBase(), "non-existent")
      ioPOSIX(target).unsafeRunSync

      target.exists() mustEqual true
      target.isDirectory() mustEqual true
    }

    "produce distinct UUIDs" in setupDir { base =>
      val test = for {
        interp <- ioPOSIX(base)
        first <- interp(GenUUID)
        second <- interp(GenUUID)
      } yield (first, second)

      val (first, second) = test.unsafeRunSync

      first must not(be(second))
    }

    "open a file and return its contents" in setupDir { base =>
      val file = new File(base, "test")

      val fos = new FileOutputStream(file)
      fos.write(Array[Byte](1, 2, 3, 4, 5))
      fos.close()

      val test = for {
        interp <- ioPOSIX(base)
        results <- interp(OpenR(Path.rootDir </> Path.file("test")))
        contents <- translate(results, interp).fold(ByteVector.empty)(_ ++ _).runLast
      } yield contents

      test.unsafeRunSync must beSome(ByteVector(1, 2, 3, 4, 5))
    }

    "write to a file" in setupDir { base =>
      val file = new File(base, "test")

      val test = for {
        interp <- ioPOSIX(base)
        sink <- interp(OpenW(Path.rootDir </> Path.file("test")))
        driver = Stream(ByteVector(1, 2, 3, 4, 5)).covary[POSIXWithIO].to(sink)
        _ <- translate(driver, interp).run
      } yield ()

      test.unsafeRunSync

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
        interp <- ioPOSIX(base)
        files <- interp(Ls(Path.rootDir))
      } yield files

      test.unsafeRunSync must containTheSameElementsAs(targets.map(Path.file))
    }

    "make a subdirectory" in setupDir { base =>
      val target = "foo"

      val test = for {
        interp <- ioPOSIX(base)
        _ <- interp(MkDir(Path.rootDir </> Path.dir(target)))
      } yield ()

      test.unsafeRunSync

      val ftarget = new File(base, target)

      ftarget.exists() mustEqual true
      ftarget.isDirectory() mustEqual true
    }

    "link two directories" in setupDir { base =>
      val from = "foo"
      val to = "bar"

      val test = for {
        interp <- ioPOSIX(base)
        _ <- interp(MkDir(Path.rootDir </> Path.dir(from)))
        _ <- interp(LinkDir(Path.rootDir </> Path.dir(from), Path.rootDir </> Path.dir(to)))
      } yield ()

      test.unsafeRunSync

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
        interp <- ioPOSIX(base)

        sink1 <- interp(OpenW(Path.rootDir </> Path.file(from)))
        driver1 = Stream(ByteVector(1, 2, 3)).covary[POSIXWithIO].to(sink1)
        _ <- translate(driver1, interp).run

        _ <- interp(LinkFile(Path.rootDir </> Path.file(from), Path.rootDir </> Path.file(to)))

        sink2 <- interp(OpenW(Path.rootDir </> Path.file(from)))
        driver2 = Stream(ByteVector(4, 5, 6)).covary[POSIXWithIO].to(sink2)
        _ <- translate(driver2, interp).run
      } yield ()

      test.unsafeRunSync

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
        interp <- ioPOSIX(base)
        _ <- interp(Move(Path.rootDir </> Path.file(from), Path.rootDir </> Path.file(to)))
      } yield ()

      test.unsafeRunSync

      new File(base, from).exists() mustEqual false
      new File(base, to).exists() mustEqual true
    }

    "check if a file exists" in setupDir { base =>
      val foo = "foo"
      val bar = "bar"

      Files.createFile(new File(base, foo).toPath())

      val test = for {
        interp <- ioPOSIX(base)
        fooEx <- interp(Exists(Path.rootDir </> Path.file(foo)))
        barEx <- interp(Exists(Path.rootDir </> Path.file(bar)))
      } yield (fooEx, barEx)

      val (fooEx, barEx) = test.unsafeRunSync

      fooEx mustEqual true
      barEx mustEqual false
    }

    "check if a directory exists" in setupDir { base =>
      val foo = "foo"
      val bar = "bar"

      new File(base, foo).mkdir()

      val test = for {
        interp <- ioPOSIX(base)
        fooEx <- interp(Exists(Path.rootDir </> Path.dir(foo)))
        barEx <- interp(Exists(Path.rootDir </> Path.dir(bar)))
      } yield (fooEx, barEx)

      val (fooEx, barEx) = test.unsafeRunSync

      fooEx mustEqual true
      barEx mustEqual false
    }

    "delete a file" in setupDir { base =>
      val target = "foo"

      Files.createFile(new File(base, target).toPath())

      val test = for {
        interp <- ioPOSIX(base)
        _ <- interp(Delete(Path.rootDir </> Path.file(target)))
      } yield ()

      test.unsafeRunSync

      new File(base, target).exists() mustEqual false
    }

    "delete an empty directory" in setupDir { base =>
      val target = "foo"
      val ftarget = new File(base, target)

      ftarget.mkdir()

      val test = for {
        interp <- ioPOSIX(base)
        _ <- interp(Delete(Path.rootDir </> Path.dir(target)))
      } yield ()

      test.unsafeRunSync

      ftarget.exists() mustEqual false
    }

    "delete a non-empty directory" in setupDir { base =>
      val target = "foo"
      val ftarget = new File(base, target)

      ftarget.mkdir()

      Files.createFile(new File(ftarget, "bar").toPath())

      val test = for {
        interp <- ioPOSIX(base)
        _ <- interp(Delete(Path.rootDir </> Path.dir(target)))
      } yield ()

      test.unsafeRunSync

      ftarget.exists() mustEqual false
    }

    "delete a non-empty dir symlink" in setupDir { base =>
      val target = "foo"
      val ftarget = new File(base, target)

      ftarget.mkdir()

      Files.createFile(new File(ftarget, "test").toPath())

      val test = for {
        interp <- ioPOSIX(base)
        _ <- interp(LinkDir(Path.rootDir </> Path.dir("foo"), Path.rootDir </> Path.dir("bar")))
        _ <- interp(Delete(Path.rootDir </> Path.dir("bar")))
      } yield ()

      test.unsafeRunSync

      ftarget.exists() mustEqual true
      new File(ftarget, "test").exists() mustEqual true

      new File(base, "bar").exists() mustEqual false
    }
  }

  def translate[A](str: Stream[POSIXWithIO, A], interp: POSIXOp ~> IO): Stream[IO, A] = {
    val nt = λ[FunctionK[POSIXWithIO, IO]] { pwt =>
      val fullInt =
        CopK.NaturalTransformation.of[POSIXWithIOCopK, IO](interp, NaturalTransformation.refl[IO])

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

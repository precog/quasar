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

import cats.effect.IO
import cats.syntax.flatMap._

import org.specs2.mutable._

import pathy.Path

import java.io.File
import java.nio.file.Files

// For cats.effect.Timer[IO]
import scala.concurrent.ExecutionContext.Implicits.global
import shims._

object SerialVFSSpecs extends Specification {
  "serial vfs facade" should {
    "create a scratch directory, assign a path, work with real files, and list" in {
      val base = Files.createTempDirectory("SerialVFSSpecs").toFile

      val test = SerialVFS[IO](base).flatMap(_(vfs => for {
        blob <- vfs.scratch
        version <- vfs.fresh(blob)
        dir <- vfs.underlyingDir(blob, version)

        _ <- IO {
          dir.exists() mustEqual true

          Files.createFile(new File(dir, "test").toPath())
        }

        _ <- vfs.commit(blob, version)
        _ <- vfs.link(blob, Path.rootDir </> Path.file("foo"))
      } yield ()))

      val test2 = SerialVFS[IO](base).flatMap(_(vfs => for {
        ob <- vfs.readPath(Path.rootDir </> Path.file("foo"))

        blob <- IO {
          ob must beSome
          ob.get
        }

        ov <- vfs.headOfBlob(blob)

        version <- IO {
          ov must beSome
          ov.get
        }

        dir <- vfs.underlyingDir(blob, version)

        _ <- IO {
          dir.exists() mustEqual true
          new File(dir, "test").exists() mustEqual true
        }
      } yield ()))

      val test3 =
        SerialVFS[IO](base).flatMap(_(_.ls(Path.rootDir)))

      val result =
        test >> test2 >> test3

      result.unsafeRunSync must_=== List(Path.file("foo"))
    }
  }
}

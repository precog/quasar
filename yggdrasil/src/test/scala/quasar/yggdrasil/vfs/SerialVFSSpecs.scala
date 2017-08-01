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

import fs2.Stream
import fs2.interop.scalaz._

import org.specs2.mutable._

import pathy.Path

import scalaz.concurrent.Task

import java.io.File
import java.nio.file.Files

object SerialVFSSpecs extends Specification {

  "serial vfs facade" should {
    "create a scratch directory, assign a path, work with real files, and list" in {
      val base = Files.createTempDirectory("SerialVFSSpecs").toFile

      val test = for {
        vfs <- SerialVFS(base)

        ta = for {
          blob <- vfs.scratch
          version <- vfs.fresh(blob)
          dir <- vfs.underlyingDir(blob, version)

          _ <- Task delay {
            dir.exists() mustEqual true

            Files.createFile(new File(dir, "test").toPath())
          }

          _ <- vfs.commit(blob, version)
          _ <- vfs.link(blob, Path.rootDir </> Path.file("foo"))
        } yield ()

        _ <- Stream.eval(ta)
      } yield ()

      test.take(1).run.unsafePerformSync

      val test2 = for {
        vfs <- SerialVFS(base)

        ta = for {
          ob <- vfs.readPath(Path.rootDir </> Path.file("foo"))

          blob <- Task delay {
            ob must beSome
            ob.get
          }

          ov <- vfs.headOfBlob(blob)

          version <- Task delay {
            ov must beSome
            ov.get
          }

          dir <- vfs.underlyingDir(blob, version)

          _ <- Task delay {
            dir.exists() mustEqual true
            new File(dir, "test").exists() mustEqual true
          }
        } yield ()

        _ <- Stream.eval(ta)
      } yield ()

      test2.take(1).run.unsafePerformSync

      val test3 = for {
        vfs <- SerialVFS(base)
        paths <- Stream.eval(vfs.ls(Path.rootDir))
      } yield paths

      val result = test3.take(1).runLast.unsafePerformSync

      result must beSome(List(Path.file("foo")))
    }
  }
}

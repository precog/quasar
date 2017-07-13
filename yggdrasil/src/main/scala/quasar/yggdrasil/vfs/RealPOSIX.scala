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

import quasar.contrib.pathy.APath
import quasar.precog.util.IOUtils

import fs2.{Chunk, Stream}
import fs2.interop.scalaz._
import fs2.io.file.{readAll, writeAll}
import fs2.util.Suspendable

import pathy.Path

import scalaz.{~>, -\/, \/-, Coproduct, Free, Inject}
import scalaz.concurrent.Task
import scalaz.std.list._
import scalaz.syntax.traverse._

import scodec.bits.ByteVector

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import java.util.UUID

object RealPOSIX {
  import POSIXOp._

  def apply(root: File): Task[POSIXOp ~> Task] = {
    def canonicalize(path: APath): File =
      new File(root, Path.posixCodec.printPath(path))

    mkdir(root) map { _ =>
      λ[POSIXOp ~> Task] {
        case GenUUID =>
          Task.delay(UUID.randomUUID())

        case OpenR(target) =>
          val ptarget = canonicalize(target).toPath()

          val stream =
            readAll[POSIXWithTask](ptarget, 4096).chunks.map(c => ByteVector(c.toArray))

          Task.now(stream)

        case OpenW(target) =>
          val ptarget = canonicalize(target).toPath()

          val cmap: Stream[POSIXWithTask, ByteVector] => Stream[POSIXWithTask, Byte] =
            _.map(_.toArray).map(Chunk.bytes).flatMap(Stream.chunk)

          val sink = writeAll[POSIXWithTask](ptarget).compose(cmap)
          Task.now(sink)

        case Ls(target) =>
          val ftarget = canonicalize(target)

          for {
            contents <- Task.delay(ftarget.list().toList)

            paths <- contents traverse { name =>
              Task.delay(new File(ftarget, name).isDirectory()) map { isd =>
                if (isd)
                  Path.dir(name)
                else
                  Path.file(name)
              }
            }
          } yield paths

        case MkDir(target) =>
          Task.delay(canonicalize(target).mkdirs()).void

        case LinkDir(src, target) =>
          val psrc = canonicalize(src).toPath()
          val ptarget = canonicalize(target).toPath()

          Task delay {
            Files.createSymbolicLink(ptarget, psrc)

            ()
          }

        case LinkFile(src, target) =>
          val psrc = canonicalize(src).toPath()
          val ptarget = canonicalize(target).toPath()

          Task delay {
            Files.createSymbolicLink(ptarget, psrc)

            ()
          }

        case Move(src, target) =>
          val psrc = canonicalize(src).toPath()
          val ptarget = canonicalize(target).toPath()

          Task delay {
            Files.move(psrc, ptarget, StandardCopyOption.ATOMIC_MOVE)

            ()
          }

        case Exists(target) =>
          Task.delay(canonicalize(target).exists())

        case Delete(target) =>
          val ftarget = canonicalize(target)

          Path.refineType(target) match {
            case -\/(dir) =>
              Task delay {
                if (Files.isSymbolicLink(ftarget.toPath()))
                  Files.delete(ftarget.toPath())
                else
                  IOUtils.recursiveDelete(ftarget).unsafePerformIO()
              }

            case \/-(file) => Task.delay(ftarget.delete()).void
          }
      }
    }
  }

  private def mkdir(root: File): Task[Unit] = Task.delay(root.mkdirs())

  private implicit def pwtSuspendable: Suspendable[POSIXWithTask] =
    new Suspendable[POSIXWithTask] {
      val I = Inject[Task, Coproduct[POSIXOp, Task, ?]]

      def pure[A](a: A): POSIXWithTask[A] =
        Free.pure(a)

      def flatMap[A, B](fa: POSIXWithTask[A])(f: A => POSIXWithTask[B]): POSIXWithTask[B] =
        fa.flatMap(f)

      def suspend[A](fa: => POSIXWithTask[A]): POSIXWithTask[A] =
        flatMap(delay(fa))(x => x)

      override def delay[A](a: => A): POSIXWithTask[A] =
        Free.liftF(I.inj(Task.delay(a)))
    }
}

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

import quasar.contrib.pathy.APath
import quasar.precog.util.IOUtils

import cats.effect.Sync
import fs2.{Chunk, Stream}
import fs2.io.file.{readAll, writeAll}

import pathy.Path

import scalaz.{~>, -\/, \/-, Scalaz}, Scalaz._
import shims._

import scodec.bits.ByteVector

import java.io.{File, IOException}
import java.nio.file.{Files, StandardCopyOption}
import java.util.UUID

object RealPOSIX {
  import POSIXOp._

  def apply[F[_]](root: File)(implicit F: Sync[F]): F[POSIXOp ~> F] = {
    def canonicalize(path: APath): File =
      new File(root, Path.posixCodec.printPath(path))

    mkdir(root) map { _ =>
      λ[POSIXOp ~> F] {
        case GenUUID =>
          F.delay(UUID.randomUUID())

        case OpenR(target) =>
          val ptarget = canonicalize(target).toPath()

          val stream =
            readAll[POSIXWithIO](ptarget, 4096).chunks.map(c => ByteVector(c.toArray))

          F.pure(stream)

        case OpenW(target) =>
          val ptarget = canonicalize(target).toPath()

          val cmap: Stream[POSIXWithIO, ByteVector] => Stream[POSIXWithIO, Byte] =
            _.map(_.toArray).map(Chunk.bytes).flatMap(Stream.chunk(_).covary[POSIXWithIO])

          val sink = writeAll[POSIXWithIO](ptarget).compose(cmap)
          F.pure(sink)

        case Ls(target) =>
          val ftarget = canonicalize(target)

          for {
            contents <- F.delay(ftarget.list().toList)

            paths <- contents traverse { name =>
              F.delay(new File(ftarget, name).isDirectory()) map { isd =>
                if (isd)
                  Path.dir(name)
                else
                  Path.file(name)
              }
            }
          } yield paths

        case MkDir(target) =>
          F.delay(canonicalize(target).mkdirs()).void

        case LinkDir(src, target) =>
          val psrc = canonicalize(src).toPath()
          val ptarget = canonicalize(target).toPath()

          F.delay {
            try {
              Files.createSymbolicLink(ptarget, psrc)

              true
            } catch {
              case _: IOException => false
            }
          }

        case LinkFile(src, target) =>
          val psrc = canonicalize(src).toPath()
          val ptarget = canonicalize(target).toPath()

          F.delay {
            try {
              Files.createSymbolicLink(ptarget, psrc)

              true
            } catch {
              case _: IOException => false
            }
          }

        case Move(src, target) =>
          val psrc = canonicalize(src).toPath()
          val ptarget = canonicalize(target).toPath()

          F.delay {
            Files.move(psrc, ptarget, StandardCopyOption.ATOMIC_MOVE)

            ()
          }

        case Exists(target) =>
          F.delay(canonicalize(target).exists())

        case Delete(target) =>
          val ftarget = canonicalize(target)

          Path.refineType(target) match {
            case -\/(dir) =>
              F.delay {
                if (Files.isSymbolicLink(ftarget.toPath()))
                  Files.delete(ftarget.toPath())
                else
                  IOUtils.recursiveDelete(ftarget).unsafePerformIO()
              }

            case \/-(file) => F.delay(ftarget.delete()).void
          }
      }
    }
  }

  private def mkdir[F[_]](root: File)(implicit F: Sync[F]): F[Unit] =
    F.delay(root.mkdirs())
}

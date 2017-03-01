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

import quasar.Predef._
import quasar.Data
import quasar.contrib.pathy._
import quasar.effect.{Kvs, MonoSeq}
import quasar.effect.uuid.UuidReader
import quasar.fp.numeric.Positive
import quasar.fs._, FileSystemError._
import quasar.physical.marklogic.qscript._
import quasar.physical.marklogic.xcc._
import quasar.physical.marklogic.xquery._

import pathy.Path._
import scalaz._, Scalaz._

object writefile {
  def interpret[F[_]: Monad: Xcc: UuidReader: PrologW: PrologL, FMT: SearchOptions](
    chunkSize: Positive
  )(
    implicit
    C:       AsContent[FMT, Data],
    SP:      StructuralPlanner[F, FMT],
    cursors: Kvs[F, WriteFile.WriteHandle, Unit],
    idSeq:   MonoSeq[F]
  ): WriteFile ~> F = {
    def write(dst: AFile, lines: Vector[Data]) = {
      val chunk = Data._arr(lines.toList)
      ops.upsertFile[F, FMT, Data](dst, chunk) map (_.fold(
        msgs => Vector(FileSystemError.writeFailed(chunk, msgs intercalate ", ")),
        _ map (xe => FileSystemError.writeFailed(chunk, xe.shows))))
    }

    λ[WriteFile ~> F] {
      case WriteFile.Open(file) =>
        for {
          wh <- idSeq.next ∘ (WriteFile.WriteHandle(file, _))
          _  <- cursors.put(wh, ())
          _  <- ops.ensureLineage[F](fileParent(file))
        } yield wh.right[FileSystemError]

      case WriteFile.Write(h, data) =>
        OptionT(cursors.get(h)).isDefined.ifM(
          data.grouped(chunkSize.get.toInt)
            .toStream
            .foldMapM(write(h.file, _)),
          Vector(unknownWriteHandle(h)).point[F])

      case WriteFile.Close(h) =>
        cursors.delete(h)
    }
  }
}

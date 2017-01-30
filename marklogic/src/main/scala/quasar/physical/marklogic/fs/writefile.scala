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

package quasar.physical.marklogic.fs

import quasar.Predef._
import quasar.Data
import quasar.effect.{Capture, Kvs, MonoSeq}
import quasar.effect.uuid.UuidReader
import quasar.fs._, FileSystemError._
import quasar.physical.marklogic.xcc._

import scalaz._, Scalaz._

object writefile {
  def interpret[F[_]: Monad: Capture: SessionReader: UuidReader: XccErr, FMT](
    implicit
    C:       AsContent[FMT, Data],
    cursors: Kvs[F, WriteFile.WriteHandle, Unit],
    idSeq:   MonoSeq[F]
  ): WriteFile ~> F = λ[WriteFile ~> F] {
    case WriteFile.Open(file) =>
      for {
        wh <- idSeq.next ∘ (WriteFile.WriteHandle(file, _))
        _  <- cursors.put(wh, ())
        r  <- ops.exists[F](file).ifM(
                ().right[PathError].point[F],
                ops.createFile[F](file))
      } yield r.leftMap(pathErr(_)).as(wh)

    case WriteFile.Write(h, data) =>
      OptionT(cursors.get(h)).isDefined.ifM(
        ops.appendToFile[F, FMT](h.file, data),
        Vector(unknownWriteHandle(h)).point[F])

    case WriteFile.Close(h) =>
      cursors.delete(h)
  }
}

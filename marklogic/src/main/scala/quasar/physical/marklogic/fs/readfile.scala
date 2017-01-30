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
import quasar.effect.{Capture, Kvs, MonoSeq}
import quasar.fp.numeric.Positive
import quasar.fs._
import quasar.fs.impl._
import quasar.physical.marklogic.xcc._

import scalaz._, Scalaz._

object readfile {
  def interpret[F[_]: Monad: Capture: CSourceReader: SessionReader: XccErr](
    chunkSize: Positive
  )(implicit
    K: Kvs[F, ReadFile.ReadHandle, Option[ResultCursor]],
    S: MonoSeq[F]
  ): ReadFile ~> F =
    readFromDataCursor[Option[ResultCursor], F] { (file, opts) =>
      ops.exists[F](file).ifM(
        ops.readFile[F](chunkSize)(file, opts.offset, opts.limit) map (some(_)),
        none[ResultCursor].point[F]
      ) map (_.right[FileSystemError])
    }
}

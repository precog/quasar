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

import quasar.effect.{Kvs, MonoSeq}
import quasar.fp.numeric.Positive
import quasar.fs._
import quasar.fs.impl._
import quasar.physical.marklogic.qscript._
import quasar.physical.marklogic.xcc._
import quasar.physical.marklogic.xquery._

import scalaz._, Scalaz._
import scalaz.stream.Process

object readfile {
  type RKvs[F[_], G[_]] = Kvs[G, ReadFile.ReadHandle, DataStream[F]]

  def interpret[
    F[_]: Monad: Catchable: Xcc: PrologL,
    G[_]: Monad: MonoSeq: RKvs[F, ?[_]],
    FMT: SearchOptions
  ](
    chunkSize: Positive, fToG: F ~> G
  )(implicit
    SP: StructuralPlanner[F, FMT]
  ): ReadFile ~> G =
    readFromProcess(fToG) { (file, opts) =>
      fToG(ops.pathHavingFormatExists[F, FMT](file) map (_.fold(
        ops.readFile[F, FMT](file, opts.offset, opts.limit)
          .chunk(chunkSize.get.toInt)
          .map(_ traverse xdmitem.decodeForFileSystem),
        (Process.empty: DataStream[F])
      ))) map (_.right[FileSystemError])
    }
}

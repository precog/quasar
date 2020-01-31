/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.impl.datasource.local

import slamdata.Predef._
import quasar.connector.LightweightDatasourceModule.DS
import quasar.connector._

import java.nio.file.{Path => JPath}

import cats.effect.{Blocker, ContextShift, Effect, Timer}
import fs2.{io, Stream}

object LocalStatefulDatasource {

  /* @param readChunkSizeBytes the number of bytes per chunk to use when reading files.
  */
  def apply[F[_]: ContextShift: Effect: MonadResourceErr: Timer](
      root: JPath,
      readChunkSizeBytes: Int,
      format: DataFormat,
      pageSize: Long,
      blocker: Blocker)
      : DS[F] = {

    EvaluableLocalDatasource[F](LocalType, root) { iRead =>

      val plate = Effect[F].delay(new LocalStatefulPlate())

      def data(more: Option[Long]): Stream[F, Byte] =
        more match {
          case Some(start) =>
            io.file.readRange[F](
              iRead.path,
              blocker,
              readChunkSizeBytes,
              start,
              start + pageSize)
          case None =>
            io.file.readRange[F](
              iRead.path,
              blocker,
              readChunkSizeBytes,
              0,
              pageSize)
        }

      def state(p: LocalStatefulPlate): F[Option[Long]] =
        Effect[F].delay(p.updateState(pageSize))

      QueryResult.stateful(
        format,
        plate,
        state(_),
        data(_),
        iRead.stages)
    }
  }
}

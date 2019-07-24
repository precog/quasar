/*
 * Copyright 2014–2019 SlamData Inc.
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
import quasar.concurrent.BlockingContext
import quasar.connector.LightweightDatasourceModule.DS
import quasar.connector._

import java.nio.file.{Path => JPath}

import cats.effect.{ContextShift, Effect, Timer}
import fs2.io
import scalaz.syntax.tag._

object LocalDatasource {

  /* @param readChunkSizeBytes the number of bytes per chunk to use when reading files.
  */
  def apply[F[_]: ContextShift: Effect: MonadResourceErr: Timer](
      root: JPath,
      readChunkSizeBytes: Int,
      format: ParsableType,
      compressionScheme: Option[CompressionScheme],
      blockingPool: BlockingContext)
      : DS[F] = {

    EvaluableLocalDatasource[F](LocalType, root) { iRead =>
      val content =
        io.file.readAll[F](iRead.path, blockingPool.unwrap, readChunkSizeBytes)

      val typedResult =
        QueryResult.typed(format, content, iRead.stages)

      compressionScheme.fold(typedResult)(QueryResult.compressed(_, typedResult))
    }
  }
}

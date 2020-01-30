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

package quasar.impl.parsing

import quasar.api.resource.ResourcePath
import quasar.connector.{MonadResourceErr, DataFormat, QueryResult}

import slamdata.Predef._

import cats.effect.Sync

import fs2.Stream

import qdata.QDataEncode

object ResourceParser {
  def apply[F[_]: Sync: MonadResourceErr, A: QDataEncode](
      path: ResourcePath,
      content: QueryResult[F])
      : Stream[F, A] =
    ResultParser[F, A](content) handleErrorWith { t =>
      expectedTypeOf(content)
        .flatMap(TectonicResourceError(path, _, t))
        .fold(Stream.raiseError(t))(re => Stream.eval(MonadResourceErr[F].raiseError(re)))
    }

  private def expectedTypeOf[F[_]](qr: QueryResult[F]): Option[DataFormat] =
    Some(qr) collect {
      case QueryResult.Typed(t, _, _) => t
    }
}

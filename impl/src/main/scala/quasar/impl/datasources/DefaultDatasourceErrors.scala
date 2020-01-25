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

package quasar.impl.datasources

import slamdata.Predef.{Exception, Option, Unit}

import quasar.Condition
import quasar.fp.ski.κ

import cats.Functor
import cats.effect.Sync
import cats.effect.concurrent.Ref
import cats.syntax.functor._

import scalaz.{IMap, Order}

final class DefaultDatasourceErrors[F[_]: Functor, I: Order] private (
    errors: Ref[F, IMap[I, Exception]])
    extends DatasourceErrors[F, I] {

  val erroredDatasources: F[IMap[I, Exception]] =
    errors.get

  def datasourceError(datasourceId: I): F[Option[Exception]] =
    erroredDatasources.map(_.lookup(datasourceId))
}

object DefaultDatasourceErrors {
  /**
   * Returns the `DatasourceErrors` impl and a callback function to invoke
   * when the condition of a `Datasource` changes.
   */
  def apply[F[_]: Sync, I: Order]
      : F[(DatasourceErrors[F, I], (I, Condition[Exception]) => F[Unit])]  =
    Ref[F].of(IMap.empty[I, Exception]) map { errors =>
      val onChange: (I, Condition[Exception]) => F[Unit] =
        (i, c) => errors.update(_.alter(i, κ(Condition.optionIso.get(c))))

      (new DefaultDatasourceErrors(errors), onChange)
    }
}

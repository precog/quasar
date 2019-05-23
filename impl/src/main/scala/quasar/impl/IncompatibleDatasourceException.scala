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

package quasar.impl

import slamdata.Predef._

import quasar.api.datasource.DatasourceType
import quasar.api.destination.DestinationType

import cats.ApplicativeError
import scalaz.syntax.either._
import scalaz.{\/, -\/, \/-}

final case class IncompatibleModuleException(kind: DatasourceType \/ DestinationType) extends java.lang.RuntimeException {
  override def getMessage = kind match {
    case -\/(k) => s"Loaded datasource implementation with type $k is incompatible with quasar"
    case \/-(k) => s"Loaded destination implementation with type $k is incompatible with quasar"
  }
}

object IncompatibleModuleException {
  def datasource(kind: DatasourceType): IncompatibleModuleException =
    IncompatibleModuleException(kind.left)

  def destination(kind: DestinationType): IncompatibleModuleException =
    IncompatibleModuleException(kind.right)

  def linkDatasource[F[_]: ApplicativeError[?[_], Throwable], A](kind: DatasourceType, fa: => F[A]): F[A] =
    link(datasource(kind), fa)

  def linkDestination[F[_]: ApplicativeError[?[_], Throwable], A](kind: DestinationType, fa: => F[A]): F[A] =
    link(destination(kind), fa)

  private def link[F[_]: ApplicativeError[?[_], Throwable], A](ex: IncompatibleModuleException, fa: => F[A]): F[A] =
    try {
      fa
    } catch {
      case _: java.lang.LinkageError => ApplicativeError[F, Throwable].raiseError(ex)
    }

}

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

package quasar.fs.mount

import slamdata.Predef._
import quasar.connector.EnvironmentError
import quasar.fp.ski.κ
import quasar.fs.{BackendEffect, FileSystemType}

import scala.StringContext

import scalaz._
import scalaz.syntax.either._
import scalaz.syntax.monadError._

import BackendDef._

final case class BackendDef[F[_]](run: FsCfg => Option[DefErrT[F, DefinitionResult[F]]]) {
  def apply(typ: FileSystemType, uri: ConnectionUri)(implicit F: Monad[F]): DefErrT[F, DefinitionResult[F]] =
    run((typ, uri)).getOrElse(NonEmptyList(
      s"Unsupported filesystem type: ${typ.value}"
    ).left[EnvironmentError].raiseError[DefErrT[F, ?], DefinitionResult[F]])

  def orElse(other: => BackendDef[F]): BackendDef[F] =
    BackendDef(cfg => run(cfg) orElse other.run(cfg))

  def translate[G[_]: Functor](f: F ~> G): BackendDef[G] =
    BackendDef(c => run(c).map(r => EitherT(f(r.run)).map(_ translate f)))
}

object BackendDef {
  type FsCfg            = (FileSystemType, ConnectionUri)
  /** Reasons why the configuration is invalid or an environment error. */
  type DefinitionError  = NonEmptyList[String] \/ EnvironmentError
  type DefErrT[F[_], A] = EitherT[F, DefinitionError, A]

  final case class DefinitionResult[F[_]](run: BackendEffect ~> F, close: F[Unit]) {
    def translate[G[_]](f: F ~> G): DefinitionResult[G] =
      DefinitionResult(f compose run, f(close))
  }

  def fromPF[F[_]](
    pf: PartialFunction[FsCfg, DefErrT[F, DefinitionResult[F]]]
  ): BackendDef[F] =
    BackendDef(pf.lift)

  implicit def backendDefMonoid[F[_]]: Monoid[BackendDef[F]] =
    new Monoid[BackendDef[F]] {
      def zero = BackendDef(κ(None))
      def append(d1: BackendDef[F], d2: => BackendDef[F]) = d1 orElse d2
    }
}

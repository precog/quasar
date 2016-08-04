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

package quasar.fs.mount

import quasar.Predef.{None, Option, PartialFunction, String, Unit}
import quasar.SKI.κ
import quasar.EnvironmentError
import quasar.fs.{FileSystem, FileSystemType}

import scala.StringContext

import scalaz._
import scalaz.syntax.either._
import scalaz.syntax.monadError._

import FileSystemDef._

final case class FileSystemDef[F[_]](run: FsCfg => Option[DefErrT[F, DefinitionResult[F]]]) {
  def apply(typ: FileSystemType, uri: ConnectionUri)(implicit F: Monad[F]): DefErrT[F, DefinitionResult[F]] =
    run((typ, uri)).getOrElse(NonEmptyList(
      s"Unsupported filesystem type: ${typ.value}"
    ).left[EnvironmentError].raiseError[DefErrT[F, ?], DefinitionResult[F]])

  def orElse(other: => FileSystemDef[F]): FileSystemDef[F] =
    FileSystemDef(cfg => run(cfg) orElse other.run(cfg))

  def translate[G[_]: Functor](f: F ~> G): FileSystemDef[G] =
    FileSystemDef(c => run(c).map(r => EitherT(f(r.run)).map(_ translate f)))
}

object FileSystemDef {
  type FsCfg            = (FileSystemType, ConnectionUri)
  /** Reasons why the configuration is invalid or an environment error. */
  type DefinitionError  = NonEmptyList[String] \/ EnvironmentError
  type DefErrT[F[_], A] = EitherT[F, DefinitionError, A]

  final case class DefinitionResult[F[_]](run: FileSystem ~> F, close: F[Unit]) {
    def translate[G[_]](f: F ~> G): DefinitionResult[G] =
      DefinitionResult(f compose run, f(close))
  }

  def fromPF[F[_]](
    pf: PartialFunction[FsCfg, DefErrT[F, DefinitionResult[F]]]
  ): FileSystemDef[F] =
    FileSystemDef(pf.lift)

  implicit def fileSystemDefMonoid[F[_]]: Monoid[FileSystemDef[F]] =
    new Monoid[FileSystemDef[F]] {
      def zero = FileSystemDef(κ(None))
      def append(d1: FileSystemDef[F], d2: => FileSystemDef[F]) = d1 orElse d2
    }
}

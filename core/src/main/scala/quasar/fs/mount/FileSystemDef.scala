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

import quasar.Predef.{PartialFunction, String, Unit}
import quasar.EnvironmentError2
import quasar.fs.{FileSystem, FileSystemType}

import scala.StringContext

import scalaz._
import scalaz.syntax.bifunctor._
import scalaz.std.tuple._

import FileSystemDef._

final case class FileSystemDef[F[_]](run: FsCfg => DefErrT[F, DefinitionResult[F]]) {
  def apply(typ: FileSystemType, uri: ConnectionUri): DefErrT[F, DefinitionResult[F]] =
    run((typ, uri))

  def translate[G[_]: Functor](f: F ~> G): FileSystemDef[G] =
    FileSystemDef(c => EitherT(f(run(c).run)).map(_.bimap(f compose _, f(_))))
}

object FileSystemDef {
  type FsCfg                  = (FileSystemType, ConnectionUri)
  /** Reasons why the configuration is invalid or an environment error. */
  type DefinitionError        = NonEmptyList[String] \/ EnvironmentError2
  type DefErrT[F[_], A]       = EitherT[F, DefinitionError, A]
  type DefinitionResult[F[_]] = (FileSystem ~> F, F[Unit])

  def fromPF[F[_]: Monad](
    pf: PartialFunction[FsCfg, DefErrT[F, DefinitionResult[F]]]
  ): FileSystemDef[F] =
    FileSystemDef(cfg => pf.lift(cfg).getOrElse(Monoid[FileSystemDef[F]].zero.run(cfg)))

  implicit def fileSystemDefMonoid[F[_]](implicit F: Monad[F]): Monoid[FileSystemDef[F]] =
    new Monoid[FileSystemDef[F]] {
      def zero = FileSystemDef { case (typ, uri) =>
        MonadError[EitherT[F,?,?], DefinitionError]
          .raiseError(\/.left(NonEmptyList(s"Unsupported filesystem type: ${typ.value}")))
      }

      // TODO{scalaz-7.2}: Use `orElse` once we upgrade to a version 7.2+ as
      //                   the version in 7.1 is broken
      def append(d1: FileSystemDef[F], d2: => FileSystemDef[F]) =
        FileSystemDef(cfg => EitherT(F.bind(d1.run(cfg).run) {
          case -\/(_) => d2.run(cfg).run
          case \/-(f) => F.point(\/.right(f))
        }))
    }
}

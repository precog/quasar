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

package quasar

import slamdata.Predef.{Map => SMap, _}
import quasar.effect.NameGenerator
import quasar.fs.Planner.{InternalError, PlannerErrorME}
import quasar.contrib.scalaz.MonadState_
import quasar.ejson.EJson
import quasar.fp._
import quasar.qscript._
import quasar.qscript.provenance.Dimensions
import quasar.qsu.QScriptUniform.ShiftTarget

import matryoshka.{Hole => _, _}
import scalaz.{Free, Functor, Show, Traverse}
import scalaz.std.string._
import scalaz.syntax.traverse._
import scalaz.syntax.show._

package object qsu {
  type QIdAccess[T[_[_]]] = IdAccess[T[EJson]]
  type QAccess[T[_[_]], A] = Access[T[EJson], A]
  type FreeAccess[T[_[_]], A] = FreeMapA[T, QAccess[T, A]]
  type QDims[T[_[_]]] = Dimensions[QProv.P[T]]
  type QSUVerts[T[_[_]]] = SMap[Symbol, QScriptUniform[T, Symbol]]

  type RevIdxM[T[_[_]], F[_]] = MonadState_[F, QSUGraph.RevIdx[T]]
  def RevIdxM[T[_[_]], F[_]](implicit ev: RevIdxM[T, F]): RevIdxM[T, F] = ev

  def AccessHole[T[_[_]]]: FreeMapA[T, QAccess[T, Hole]] =
    Free.pure(Access.valueHole(SrcHole))

  def LeftTarget[T[_[_]]]: FreeMapA[T, ShiftTarget[T]] =
    Free.pure(ShiftTarget.LeftTarget[T]())

  def RightTarget[T[_[_]]]: FreeMapA[T, ShiftTarget[T]] =
    Free.pure(ShiftTarget.RightTarget[T]())

  def AccessLeftTarget[T[_[_]]](f: Hole => QAccess[T, Hole]): FreeMapA[T, ShiftTarget[T]] =
    Free.pure(ShiftTarget.AccessLeftTarget(f(SrcHole)))

  def AccessValueF[T[_[_]], A](a: A): FreeAccess[T, A] =
    Free.pure[MapFunc[T, ?], QAccess[T, A]](Access.Value(a))

  def AccessValueHoleF[T[_[_]]]: FreeAccess[T, Hole] =
    AccessValueF[T, Hole](SrcHole)

  def freshSymbol[F[_]: NameGenerator: Functor](prefix: String): F[Symbol] =
    NameGenerator[F].prefixedName(prefix) map (Symbol(_))

  def printMultiline[F[_]: Traverse, K: Show, V: Show](fkv: F[(K, V)]): String =
    fkv map { case (k, v) => s"  ${k.shows} -> ${v.shows}" } intercalate "\n"

  def taggedInternalError[F[_]: PlannerErrorME, A](tag: String, fa: F[A]): F[A] =
    PlannerErrorME[F].handleError(fa)(e => PlannerErrorME[F].raiseError(e match {
      case InternalError(msg, cause) => InternalError(s"[$tag] $msg", cause)
      case other => other
    }))
}

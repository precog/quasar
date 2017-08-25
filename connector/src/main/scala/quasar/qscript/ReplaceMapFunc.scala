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

package quasar.qscript

import slamdata.Predef._
import quasar.fp._

import matryoshka._
import matryoshka.patterns._
import matryoshka.data._
import matryoshka.implicits._
import scalaz._, Scalaz._

trait ReplaceMapFunc[T[_[_]], F[_]] {
  def replace(name: Symbol, repl: FreeMap[T]): NTComp[F, State[Boolean, ?]]
}

trait ReplaceMapFuncInstances {
  type CoMapFuncR[T[_[_]], A] = CoEnv[A, MapFunc[T, ?], FreeMapA[T, A]]

  private def replaceJoinSideƒ[T[_[_]], A](name: Symbol, repl: FreeMapA[T, A])
      : CoMapFuncR[T, A] => State[Boolean, CoMapFuncR[T, A]] =
    _.run match {
      case \/-(MFC(MapFuncsCore.JoinSideName(`name`))) =>
        constantState(CoEnv(repl.resume.swap), true)
      case x => state(CoEnv(x))
    }

  def replaceJoinSide[T[_[_]], A](name: Symbol, func: FreeMapA[T, A], repl: FreeMapA[T, A])
      : State[Boolean, FreeMapA[T, A]] =
    func.transCataM[State[Boolean, ?], FreeMapA[T, A], CoEnv[A, MapFunc[T, ?], ?]](replaceJoinSideƒ[T, A](name, repl))

  implicit def const[T[_[_]], A]: ReplaceMapFunc[T, Const[A, ?]] =
    ReplaceMapFunc.replaceNone[T, Const[A, ?]]

  implicit def qscriptCore[T[_[_]]]
      : ReplaceMapFunc[T, QScriptCore[T, ?]] =
    new ReplaceMapFunc[T, QScriptCore[T, ?]] {
      def replace(name: Symbol, repl: FreeMap[T]) =
        λ[QScriptCore[T, ?] ~> (State[Boolean, ?] ∘ QScriptCore[T, ?])#λ] {
          case Map(src, f) => replaceJoinSide(name, f, repl).map(Map(src, _))
          case LeftShift(src, s, i, r) => replaceJoinSide(name, s, repl).map(LeftShift(src, _, i, r))
          case x => state(x)
        }
    }

  implicit def projectBucket[T[_[_]]]
      : ReplaceMapFunc[T, ProjectBucket[T, ?]] =
    ReplaceMapFunc.replaceNone[T, ProjectBucket[T, ?]]

  implicit def thetaJoin[T[_[_]]]
      : ReplaceMapFunc[T, ThetaJoin[T, ?]] =
    ReplaceMapFunc.replaceNone[T, ThetaJoin[T, ?]]

  implicit def equiJoin[T[_[_]]]
      : ReplaceMapFunc[T, EquiJoin[T, ?]] =
    ReplaceMapFunc.replaceNone[T, EquiJoin[T, ?]]

  implicit def coproduct[T[_[_]], F[_], G[_]]
    (implicit F: ReplaceMapFunc[T, F], G: ReplaceMapFunc[T, G])
      : ReplaceMapFunc[T, Coproduct[F, G, ?]] =
    new ReplaceMapFunc[T, Coproduct[F, G, ?]] {
      def replace(name: Symbol, repl: FreeMap[T]) =
        λ[Coproduct[F, G, ?] ~> (State[Boolean, ?] ∘ Coproduct[F, G, ?])#λ](
          _.run.bitraverse(F.replace(name, repl)(_), G.replace(name, repl)(_)) ∘ (Coproduct(_)))
  }
}

object ReplaceMapFunc extends ReplaceMapFuncInstances {
  def replaceNone[T[_[_]], F[_]]: ReplaceMapFunc[T, F] =
    new ReplaceMapFunc[T, F] {
      def replace(name: Symbol, repl: FreeMap[T]) =
        λ[F ~> (State[Boolean, ?] ∘ F)#λ](state(_))
    }

  /* Returns a value if a `JoinSideName` with `name` was replaced by `repl` in `branch`.
   */
  def applyToBranch[T[_[_]]: BirecursiveT](
    name: Symbol,
    branch: FreeQS[T],
    repl: FreeMap[T])(
    implicit R: ReplaceMapFunc[T, QScriptTotal[T, ?]])
      : Option[FreeQS[T]] = {
    def res(qs: QScriptTotal[T, FreeQS[T]])
        : State[Boolean, CoEnvQS[T, FreeQS[T]]] =
      R.replace(name, repl)(qs).map(coenvPrism.reverseGet(_))

    branch.transCataM(liftCoM(res)).run(false) match {
      case (true, x) => Some(x)
      case (false, _) => None
    }
  }

  def applyToFunc[T[_[_]]: BirecursiveT, A](
    name: Symbol,
    func: FreeMapA[T, A],
    repl: FreeMapA[T, A])
      : Option[FreeMapA[T, A]] =
    replaceJoinSide(name, func, repl).run(false) match {
      case (true, x) => Some(x)
      case (false, _) => None
    }
}

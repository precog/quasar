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

package quasar.frontend

import slamdata.Predef._
import quasar._
import quasar.common.{phase, phaseM, JoinType, PhaseResultTell, SortDir}
import quasar.common.data.Data
import quasar.common.effect.NameGenerator
import quasar.contrib.pathy.FPath
import quasar.contrib.scalaz.MonadError_
import quasar.time.TemporalPart

import scala.Symbol

import matryoshka.{Corecursive, Recursive}
import monocle.Prism
import shapeless.Nat
import scalaz._
import scalaz.syntax.monad._

package object logicalplan {
  def read[A] =
    Prism.partial[LogicalPlan[A], FPath] { case Read(p) => p } (Read(_))

  def constant[A] =
    Prism.partial[LogicalPlan[A], Data] { case Constant(p) => p } (Constant(_))

  // TODO this Prism is incorrect
  // it needs to guarantee the same N <: Nat everywhere
  def invoke[A] =
    Prism.partial[LogicalPlan[A], (GenericFunc[Nat], Func.Input[A, Nat])] {
      case Invoke(f, as) => (f, as)
    } ((Invoke[Nat, A](_, _)).tupled)

  def joinSideName[A] =
    Prism.partial[LogicalPlan[A], Symbol] { case JoinSideName(n) => n } (JoinSideName(_))

  def join[A] =
    Prism.partial[LogicalPlan[A], (A, A, JoinType, JoinCondition[A])] {
      case Join(l, r, t, c) => (l, r, t, c)
    } ((Join[A](_, _, _, _)).tupled)

  def free[A] =
    Prism.partial[LogicalPlan[A], Symbol] { case Free(n) => n } (Free(_))

  def let[A] =
    Prism.partial[LogicalPlan[A], (Symbol, A, A)] {
      case Let(v, f, b) => (v, f, b)
    } ((Let[A](_, _, _)).tupled)

  def sort[A] =
    Prism.partial[LogicalPlan[A], (A, NonEmptyList[(A, SortDir)])] {
      case Sort(a, ords) => (a, ords)
    } ((Sort[A](_, _)).tupled)

  def temporalTrunc[A] =
    Prism.partial[LogicalPlan[A], (TemporalPart, A)] {
      case TemporalTrunc(part, a) => (part, a)
    } ((TemporalTrunc[A](_, _)).tupled)

  def typecheck[A] =
    Prism.partial[LogicalPlan[A], (A, Type, A, A)] {
      case Typecheck(e, t, c, f) => (e, t, c, f)
    } ((Typecheck[A](_, _, _, _)).tupled)

  type ArgumentErrors = NonEmptyList[ArgumentError]

  type MonadArgumentErrs[F[_]] = MonadError_[F, ArgumentErrors]

  def MonadArgumentErrs[F[_]](implicit ev: MonadArgumentErrs[F]): MonadArgumentErrs[F] = ev

  def freshSym[F[_]: Functor: NameGenerator](prefix: String): F[Symbol] =
    NameGenerator[F].prefixedName("__" + prefix).map(Symbol(_))

  /** Optimizes and typechecks a `LogicalPlan` returning the improved plan. */
  def preparePlan[
      F[_]: Monad: MonadArgumentErrs: PhaseResultTell,
      T: Equal: RenderTree](
      t: T)(
      implicit
      TC: Corecursive.Aux[T, LogicalPlan],
      TR: Recursive.Aux[T, LogicalPlan])
      : F[T] = {

    val optimizer = new Optimizer[T]
    val lpr = optimizer.lpr

    for {
      optimized   <- phase[F]("Optimized", optimizer.optimize(t))
      typechecked <- phaseM[F]("Typechecked", lpr.ensureCorrectTypes[F](optimized))
      rewritten   <- phase[F]("Rewritten Joins", optimizer.rewriteJoins(typechecked))
    } yield rewritten
  }
}

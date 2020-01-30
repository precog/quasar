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

package quasar.frontend.logicalplan

import slamdata.Predef._
import quasar._
import quasar.common.{CIName, JoinType, SortDir}
import quasar.common.data.Data
import quasar.contrib.pathy._
import quasar.fp.ski._
import quasar.frontend.{logicalplan => lp}, lp.{LogicalPlan => LP}

import scala.Symbol

import matryoshka._
import matryoshka.implicits._
import scalaz._, Scalaz._
import shapeless.{nat, Nat, Sized}

final class LogicalPlanR[T](implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP]) {
  import quasar.std.StdLib, StdLib._
  import quasar.time.TemporalPart

  def read(path: FPath) = lp.read[T](path).embed

  def constant(data: Data) = lp.constant[T](data).embed

  def invoke[N <: Nat](func: GenericFunc[N], values: Func.Input[T, N]) =
    Invoke(func, values).embed

  def invoke1(func: GenericFunc[nat._1], v1: T) =
    invoke[nat._1](func, Func.Input1(v1))

  def invoke2(func: GenericFunc[nat._2], v1: T, v2: T) =
    invoke[nat._2](func, Func.Input2(v1, v2))

  def invoke3(func: GenericFunc[nat._3], v1: T, v2: T, v3: T) =
    invoke[nat._3](func, Func.Input3(v1, v2, v3))

  def joinSideName(name: Symbol) = lp.joinSideName[T](name).embed

  def join(left: T, right: T, tpe: JoinType, cond: JoinCondition[T]) =
    lp.join(left, right, tpe, cond).embed

  def free(name: Symbol) = lp.free[T](name).embed

  def let(name: Symbol, form: T, in: T) =
    lp.let(name, form, in).embed

  def sort(src: T, order: NonEmptyList[(T, SortDir)]) =
    lp.sort(src, order).embed

  def temporalTrunc(part: TemporalPart, src: T) =
    lp.temporalTrunc(part, src).embed

  object ArrayInflation {
    def unapply[N <: Nat](func: GenericFunc[N]): Option[UnaryFunc] =
      some(func) collect {
        case structural.FlattenArray => structural.FlattenArray
        case structural.FlattenArrayIndices => structural.FlattenArrayIndices
        case structural.ShiftArray => structural.ShiftArray
        case structural.ShiftArrayIndices => structural.ShiftArrayIndices
      }
  }

  object MapInflation {
    def unapply[N <: Nat](func: GenericFunc[N]): Option[UnaryFunc] =
      some(func) collect {
        case structural.FlattenMap => structural.FlattenMap
        case structural.FlattenMapKeys => structural.FlattenMapKeys
        case structural.ShiftMap => structural.ShiftMap
        case structural.ShiftMapKeys => structural.ShiftMapKeys
      }
  }

  // NB: this can't currently be generalized to Binder, because the key type
  //     isn't exposed there.
  def renameƒ[M[_]: Monad](f: Symbol => M[Symbol])
      : CoalgebraM[M, LP, (Map[Symbol, Symbol], T)] = {
    case (bound, t) =>
      t.project match {
        case Let(sym, expr, body) =>
          f(sym).map(sym1 =>
            lp.let(sym1, (bound, expr), (bound + (sym -> sym1), body)))
        case Free(sym) =>
          lp.free(bound.get(sym).getOrElse(sym)).point[M]
        case t => t.strengthL(bound).point[M]
      }
  }

  def rename[M[_]: Monad](f: Symbol => M[Symbol])(t: T): M[T] =
    (Map[Symbol, Symbol](), t).anaM[T](renameƒ(f))

  def normalizeTempNames(t: T) =
    rename(κ(freshSym[State[Long, ?]]("tmp")))(t).evalZero[Long]

  def bindFree(vars: Map[CIName, T])(t: T): T =
    t.cata[T] {
      case Free(sym) => vars.get(CIName(sym.name)).getOrElse((Free(sym):LP[T]).embed)
      case other     => other.embed
    }

  /** Per the following:
    * 1. Successive Lets are re-associated to the right:
    *    (let a = (let b = x1 in x2) in x3) becomes
    *    (let b = x1 in (let a = x2 in x3))
    * 2. Lets are "hoisted" outside of Invoke  nodes:
    *    (add (let a = x1 in x2) (let b = x3 in x4)) becomes
    *    (let a = x1 in (let b = x3 in (add x2 x4))
    * Note that this is safe only if all bound names are unique; otherwise
    * it could create spurious shadowing. normalizeTempNames is recommended.
    * NB: at the moment, Lets are only hoisted one level.
    */
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  val normalizeLetsƒ: LP[T] => Option[LP[T]] = {
    case Let(b, Embed(Let(a, x1, x2)), x3) =>
      lp.let(a, x1, let(b, x2, x3)).some

    // TODO generalize the following three `GenericFunc` cases
    case InvokeUnapply(func @ UnaryFunc(_, _, _), Sized(a1)) => a1 match {
      case Embed(Let(a, x1, x2)) =>
        lp.let(a, x1, invoke[nat._1](func, Func.Input1(x2))).some
      case _ => None
    }

    case InvokeUnapply(func @ BinaryFunc(_, _, _), Sized(a1, a2)) => (a1, a2) match {
      case (Embed(Let(a, x1, x2)), a2) =>
        lp.let(a, x1, invoke[nat._2](func, Func.Input2(x2, a2))).some
      case (a1, Embed(Let(a, x1, x2))) =>
        lp.let(a, x1, invoke[nat._2](func, Func.Input2(a1, x2))).some
      case _ => None
    }

    // NB: avoids illegally rewriting the continuation
    case InvokeUnapply(relations.Cond, Sized(a1, a2, a3)) => (a1, a2, a3) match {
      case (Embed(Let(a, x1, x2)), a2, a3) =>
        lp.let(a, x1, invoke[nat._3](relations.Cond, Func.Input3(x2, a2, a3))).some
      case _ => None
    }

    case InvokeUnapply(func @ TernaryFunc(_, _, _), Sized(a1, a2, a3)) => (a1, a2, a3) match {
      case (Embed(Let(a, x1, x2)), a2, a3) =>
        lp.let(a, x1, invoke[nat._3](func, Func.Input3(x2, a2, a3))).some
      case (a1, Embed(Let(a, x1, x2)), a3) =>
        lp.let(a, x1, invoke[nat._3](func, Func.Input3(a1, x2, a3))).some
      case (a1, a2, Embed(Let(a, x1, x2))) =>
        lp.let(a, x1, invoke[nat._3](func, Func.Input3(a1, a2, x2))).some
      case _ => None
    }

    case Join(l, r, tpe, cond) =>
      (l, r) match {
        case (Embed(Let(a, x1, x2)), r) =>
          lp.let(a, x1, join(x2, r, tpe, cond)).some
        case (l, Embed(Let(a, x1, x2))) =>
          lp.let(a, x1, join(l, x2, tpe, cond)).some
        case _ => None
      }

    case t => None
  }

  def normalizeLets(t: T) = t.transAna[T](repeatedly(normalizeLetsƒ))

  /** The set of paths referenced in the given plan. */
  def paths(lp: T): ISet[FPath] =
    lp.foldMap(_.cata[ISet[FPath]] {
      case Read(p) => ISet singleton p
      case other   => other.fold
    })

  /** The set of absolute paths referenced in the given plan. */
  def absolutePaths(lp: T): ISet[APath] =
    paths(lp) foldMap (p => ISet fromFoldable refineTypeAbs(p).swap)
}

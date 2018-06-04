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

package quasar.qscript.rewrites

import slamdata.Predef.{Array, Option, SuppressWarnings}
import quasar.Type
import quasar.ejson.implicits._
import quasar.contrib.iota.{copkEqual, copkTraverse}
import quasar.qscript.{
  ExtractFunc,
  ExtractFuncDerived,
  FreeMapA,
  MFC,
  MFD,
  MapFuncCore,
  MapFuncsCore,
  MapFuncsDerived,
  TTypes
}
import MapFuncCore.{rollMF, CoMapFuncR}
import MapFuncsCore.{Guard, Undefined}
import MapFuncsDerived.Typecheck

import matryoshka._
import matryoshka.data.free._
import matryoshka.implicits._
import matryoshka.patterns.CoEnv
import scalaz.{\/, Equal, Free, NonEmptyList}
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.syntax.foldable._
import scalaz.syntax.std.option._

final class DedupeGuards[T[_[_]]: BirecursiveT: EqualT] private () extends TTypes[T] {
  def apply[A: Equal](mf: CoMapFuncR[T, A]): Option[CoMapFuncR[T, A]] =
    some(mf) collect {
      case DedupeGuards.Guarded(preds @ NonEmptyList((c, t), _), cont) =>
        val substituted = cont.elgotApo[FreeMapA[A]](substituteGuardedExpression(preds))
        rollMF[T, A](MFC(Guard(c, t, substituted, Free.roll(MFC(Undefined())))))
    }

  ////

  private def guardedExpression[A: Equal](
      preds: NonEmptyList[(FreeMapA[A], Type)],
      fm: FreeMapA[A])
      : Option[FreeMapA[A]] =
    some(fm) collect {
      case ExtractFunc(Guard(c, t, e, ExtractFunc(Undefined()))) if preds.element((c, t)) => e
      case ExtractFuncDerived(Typecheck(c, t)) if preds.element((c, t)) => c
    }

  private def substituteGuardedExpression[A: Equal](
      preds: NonEmptyList[(FreeMapA[A], Type)])
      : ElgotCoalgebra[FreeMapA[A] \/ ?, CoEnv[A, MapFunc, ?], FreeMapA[A]] =
    fm => guardedExpression(preds, fm) <\/ fm.project
}

object DedupeGuards {
  def apply[T[_[_]]: BirecursiveT: EqualT, A: Equal]: CoMapFuncR[T, A] => Option[CoMapFuncR[T, A]] =
    new DedupeGuards[T].apply[A] _

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  object Guarded {
    def unapply[T[_[_]]: BirecursiveT, A](f: CoMapFuncR[T, A]): Option[(NonEmptyList[(FreeMapA[T, A], Type)], FreeMapA[T, A])] =
      f.run.toOption collect {
        case MFC(Guard(c @ Embed(Guarded(conds, _)), t, e, ExtractFunc(Undefined()))) =>
          ((c, t) <:: conds, e)

        case MFC(Guard(c, t, e, ExtractFunc(Undefined()))) =>
          (NonEmptyList((c, t)), e)

        case MFD(Typecheck(c @ Embed(Guarded(conds, _)), t)) =>
          ((c, t) <:: conds, c)

        case MFD(Typecheck(c, t)) =>
          (NonEmptyList((c, t)), c)
      }
  }
}

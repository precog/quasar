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

import slamdata.Predef.Option
import quasar.Type
import quasar.ejson.implicits._
import quasar.contrib.iota.{copkEqual, copkTraverse}
import quasar.qscript.{
  construction,
  ExtractFunc,
  MFC,
  MapFuncCore,
  MapFuncsCore,
  TTypes
}
import MapFuncCore.{rollMF, CoMapFuncR}

import matryoshka._
import matryoshka.data.free._
import scalaz.{\/, Equal, Free, IList, Validation, Writer}
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.syntax.bind._
import scalaz.syntax.either._
import scalaz.syntax.traverse._
import scalaz.syntax.validation._

final class ExtractFiltering[T[_[_]]: BirecursiveT: EqualT] private () extends TTypes[T] {
  import MapFuncsCore._

  val func = construction.Func[T]

  case class Filtering[A](p: (FreeMapA[A], Type) \/ FreeMapA[A]) {
    def build(res: Validation[FreeMapA[A], FreeMapA[A]]): MapFunc[FreeMapA[A]] = {
      val (s, f) = res.fold((func.Undefined[A], _), (_, func.Undefined[A]))

      MFC(p.fold({ case (e, t) => Guard(e, t, s, f) }, Cond(_, s, f)))
    }
  }

  object Filtering {
    implicit def equal[A: Equal]: Equal[Filtering[A]] =
      Equal.equalBy(_.p)
  }

  def apply[A: Equal](mf: CoMapFuncR[T, A]): Option[CoMapFuncR[T, A]] =
    mf.run.toOption >>= (MFC.unapply) >>= {
      // NB: The last case pulls conditionals into a wider scope, and we
      //     want to avoid doing that for certain MapFuncs, so we skip them
      //     here.
      case Guard(_, _, _, _)
         | Cond(_, _, _)
         | IfUndefined(_, _)
         | MakeArray(_)
         | MakeMap(_, _)
         | Or(_, _) => none

      case func =>
        val extracted =
          func.traverse[Writer[IList[Validation[Filtering[A], Filtering[A]]], ?], FreeMapA[A]] {
            case ExtractFunc(Guard(e, t, s, ExtractFunc(Undefined()))) =>
              Writer(IList(Filtering((e, t).left).success), s)

            case ExtractFunc(Guard(e, t, ExtractFunc(Undefined()), f)) =>
              Writer(IList(Filtering((e, t).left).failure), f)

            case ExtractFunc(Cond(p, s, ExtractFunc(Undefined()))) =>
              Writer(IList(Filtering(p.right).success), s)

            case ExtractFunc(Cond(p, ExtractFunc(Undefined()), f)) =>
              Writer(IList(Filtering(p.right).failure), f)

            case arg => Writer(IList(), arg)
          }

        extracted.written.toNel map { filters =>
          rollMF[T, A](filters.distinctE.foldRight(MFC(extracted.value)) {
            case (filter, v) =>
              val fm = Free.roll(v)
              filter.fold(_.build(fm.failure), _.build(fm.success))
          })
        }
    }
}

object ExtractFiltering {
  /** Pulls "filtering" conditionals (Cond or Guard where one branch is
    * `Undefined`) as far towards the root of the expression as possible.
    */
  def apply[T[_[_]]: BirecursiveT: EqualT, A: Equal]
      : CoMapFuncR[T, A] => Option[CoMapFuncR[T, A]] =
    new ExtractFiltering[T].apply[A] _
}

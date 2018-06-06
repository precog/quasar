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

package quasar.tpe

import slamdata.Predef._
import quasar.contrib.matryoshka._
import quasar.ejson.{Arr => JArr, CommonEJson, EJson, ExtEJson, Map => JMap}
import quasar.contrib.iota.copkTraverse

import matryoshka._
import matryoshka.implicits._
import scalaz._, Scalaz._

object normalization {
  import TypeF._
  import EJson.{fromCommon, fromExt}

  /** Returns the normal form of the given type. */
  object normalize {
    def apply[J] = new PartiallyApplied[J]
    final class PartiallyApplied[J] {
      def apply[T](t: T)(
        implicit
        J : Order[J],
        TC: Corecursive.Aux[T, TypeF[J, ?]],
        TR: Recursive.Aux[T, TypeF[J, ?]],
        JC: Corecursive.Aux[J, EJson],
        JR: Recursive.Aux[J, EJson]
      ): T =
        t.transCata[T](normalizeƒ[J, T])
    }
  }

  def normalizeƒ[J: Order, T](
    implicit
    TC: Corecursive.Aux[T, TypeF[J, ?]],
    TR: Recursive.Aux[T, TypeF[J, ?]],
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson]
  ): TypeF[J, T] => TypeF[J, T] =
    reduceToBottom[J, T] >>>
    normalizeEJson[J, T] >>>
    liftConst[J, T]      >>>
    coalesceUnion[J, T]  >>>
    simplifyUnion[J, T]

  // Normalizations

  /** Coalesce nested unions into a single union. */
  def coalesceUnion[J, T](
    implicit
    TC: Corecursive.Aux[T, TypeF[J, ?]],
    TR: Recursive.Aux[T, TypeF[J, ?]]
  ): TypeF[J, T] => TypeF[J, T] = {
    val coalesceƒ: ElgotAlgebra[(T, ?), TypeF[J, ?], NonEmptyList[T]] = {
      case (_, Unioned(ts)) => ts.join
      case (t,           _) => NonEmptyList(t)
    }

    tf => unionOf[J](tf.embed.elgotPara(coalesceƒ)).project
  }

  /** Elide `bottom` values from unions. */
  def elideBottom[J, T](
    implicit
    TR: Recursive.Aux[T, TypeF[J, ?]],
    TC: Corecursive.Aux[T, TypeF[J, ?]]
  ): TypeF[J, T] => TypeF[J, T] = totally {
    case Unioned(ts) =>
      ts.list.filterNot(isBottom[J](_)).toNel.cata(unionOf[J](_).project, bottom())
  }

  /** Lift arrays and maps of constants to constant types. */
  def liftConst[J, T](
    implicit
    TR: Recursive.Aux[T, TypeF[J, ?]],
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson]
  ): TypeF[J, T] => TypeF[J, T] = totally {
    case a @ Arr(-\/(ts)) =>
      ts.traverse(t => const[J, T].getOption(t.project))
        .cata(js => const[J, T](fromCommon(JArr(js.toList))), a)

    case m @ Map(kn, None) =>
      kn.traverse(t => const[J, T].getOption(t.project))
        .cata(jjs => const[J, T](fromExt(JMap(jjs.toList))), m)
  }

  /** Lower constant arrays and maps to types of constants. */
  def lowerConst[J: Order, T](
    implicit
    TC: Corecursive.Aux[T, TypeF[J, ?]],
    TR: Recursive.Aux[T, TypeF[J, ?]],
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson]
  ): TypeF[J, T] => TypeF[J, T] = totally {
    case Const(Embed(CommonEJson(JArr(js)))) =>
      arr[J, T](js.foldRight(IList[T]())((j, ts) =>
        const[J, T](j).embed :: ts).left)

    case Const(Embed(ExtEJson((JMap(tts))))) =>
      map[J, T](tts.foldLeft(IMap.empty[J, T])((m, kv) =>
        m + kv.map(j => const[J, T](j).embed)), none)
  }

  /** Normalizes EJson literals by
    *   - Replacing `Meta` nodes with their value component.
    */
  def normalizeEJson[J: Order, T](
    implicit
    TC: Corecursive.Aux[T, TypeF[J, ?]],
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson]
  ): TypeF[J, T] => TypeF[J, T] = {
    val norm: J => J =
      _.transCata[J](EJson.elideMetadata[J])

    totally {
      case Const(j)     => const[J, T](norm(j))
      case Map(kn, unk) => map[J, T](kn mapKeys norm, unk)
    }
  }

  /** Reduce arrays and maps containing `bottom` values to `bottom`. */
  def reduceToBottom[J, T](
    implicit TR: Recursive.Aux[T, TypeF[J, ?]]
  ): TypeF[J, T] => TypeF[J, T] = totally {
    case Arr(-\/(ts)) if ts.any(isBottom[J](_)) => bottom[J, T]()
    case Arr(\/-(Embed(Bottom())))              => bottom[J, T]()
    case Map(kn, _)   if kn.any(isBottom[J](_)) => bottom[J, T]()
    case Map(_, Some((Embed(Bottom()), _)))     => bottom[J, T]()
    case Map(_, Some((_, Embed(Bottom()))))     => bottom[J, T]()
  }

  /** Reduce unions containing `top` to `top`. */
  def reduceToTop[J, T](
    implicit T: Recursive.Aux[T, TypeF[J, ?]]
  ): TypeF[J, T] => TypeF[J, T] =
    orOriginal(ts => Unioned.unapply(ts) flatMap (_.findLeft(isTop[J](_))) map (_.project))

  /** Simplify unions by eliding subtypes. */
  def simplifyUnion[J: Order, T](
    implicit
    TC: Corecursive.Aux[T, TypeF[J, ?]],
    TR: Recursive.Aux[T, TypeF[J, ?]],
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson]
  ): TypeF[J, T] => TypeF[J, T] = {
    // TODO: How to represent this as a fold?
    @tailrec
    def elideSubtypes(in: IList[T], disjoint: IList[T]): IList[T] =
      in match {
        case INil()       =>
          disjoint
        case ICons(t, ts) =>
          if (ts.any(isSubtypeOf[J](t, _)) || disjoint.any(isSubtypeOf[J](t, _)))
            elideSubtypes(ts, disjoint)
          else
            elideSubtypes(ts, t :: disjoint)
      }

    totally { case Unioned(ts) =>
      unionOf[J](elideSubtypes(ts.list, IList()).toNel | ts).project
    }
  }
}

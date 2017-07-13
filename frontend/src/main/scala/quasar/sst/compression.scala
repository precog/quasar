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

package quasar.sst

import slamdata.Predef._
import quasar.contrib.matryoshka._
import quasar.ejson.{EJson, CommonEJson => C, Str}
import quasar.fp.numeric._
import quasar.fp.ski.ι
import quasar.tpe._

import matryoshka.{project => _, _}
import matryoshka.data.Fix
import matryoshka.patterns.EnvT
import matryoshka.implicits._
import monocle.Fold
import monocle.syntax.fields._
import scalaz._, Scalaz._
import spire.algebra.{Field, Ring}
import spire.math.ConvertableTo

object compression {
  import TypeF._
  private val TS = TypeStat

  /** Compress a map having greater than `maxSize` keys by moving the largest
    * group of keys having the same primary type to the unknown key field
    * and their values to the unknown value field.
    */
  def coalesceKeys[J: Order, A: Order: Field: ConvertableTo](
    maxSize: Positive
  )(implicit
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson]
  ): SSTF[J, A, SST[J, A]] => SSTF[J, A, SST[J, A]] = totally {
    case EnvT((ts, Map(kn, unk))) if kn.size > maxSize.value =>
      val grouped =
        kn.foldlWithKey(IMap.empty[PrimaryType, J ==>> Unit])((m, j, _) =>
          m.alter(
            primaryTypeOf(j),
            _ map (_ insert (j, ())) orElse some(IMap.singleton(j, ()))))

      val (kn1, unk1) = grouped.maximumBy(_.size).fold((kn, unk)) { m =>
        val toCompress = kn intersection m
        (kn \\ toCompress, some(compressMap(toCompress)) |+| unk)
      }

      envT(ts, map[J, SST[J, A]](kn1, unk1))
  }

  /** Compress unions by combining any constants with their primary type if it
    * also appears in the union.
    */
  def coalescePrimary[J: Order, A: Order: Field: ConvertableTo](
    implicit
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson]
  ): SSTF[J, A, SST[J, A]] => SSTF[J, A, SST[J, A]] = totally {
    case sstf @ EnvT((ts, Unioned(xs))) if xs.any(sstConst[J, A].isEmpty) =>
      val grouped = xs.list groupBy { sst =>
        sstConst[J, A].isEmpty(sst).fold(
          TypeF.primary[J](sst.project.lower),
          none)
      }

      grouped.minView flatMap { case (nonPrimary, m0) =>
        val coalesced = nonPrimary.foldLeft(m0 map (_.suml1)) { (m, sst) =>
          TypeF.primary[J](sst.project.lower) flatMap { pt =>
            m.member(some(pt)) option m.adjust(some(pt), _ |+| widenConst(sst))
          } getOrElse m.updateAppend(none, sst)
        }
        coalesced.suml1Opt map (csst => envT(ts, csst.project.lower))
      } getOrElse sstf
  }

  /** Compress maps having unknown keys by coalescing known keys with unknown
    * when the unknown contains any values having the same primary type as the
    * known key.
    */
  def coalesceWithUnknown[J: Order, A: Order: Field: ConvertableTo](
    implicit
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson]
  ): SSTF[J, A, SST[J, A]] => SSTF[J, A, SST[J, A]] = totally {
    case EnvT((ts, Map(xs, Some((k, v))))) =>
      val unkPrimaries = k.toType[Fix[TypeF[J, ?]]].project match {
        case Unioned(ts) => ISet.fromFoldable(ts.map(t => primary(t.project)).list.unite)
        case other       => ISet.fromFoldable(primary(other))
      }

      val (toCompress, unchanged) = xs partitionWithKey { (j, _) =>
        unkPrimaries member primaryTypeOf(j)
      }

      envT(ts, TypeF.map(unchanged, some(compressMap(toCompress) |+| ((k, v)))))
  }

  /** Replace statically known arrays longer than the given limit with an array
    * of unknown size.
    */
  def limitArrays[J: Order, A: Order](maxLength: Positive)(
    implicit
    A : Field[A],
    JR: Recursive.Aux[J, EJson]
  ): SSTF[J, A, SST[J, A]] => SSTF[J, A, SST[J, A]] = totally {
    case EnvT((ts, Arr(-\/(elts)))) if elts.length > maxLength.value =>
      val (cnt, len) = (size1(ts), A fromInt elts.length)
      envT(some(TS.coll(cnt, some(len), some(len))), arr(\/-(elts.suml)))
  }

  /** Replace literal string types longer than the given limit with `char[]`. */
  def limitStrings[J, A](maxLength: Positive)(
    implicit
    A : Ring[A],
    JR: Recursive.Aux[J, EJson]
  ): SSTF[J, A, SST[J, A]] => SSTF[J, A, SST[J, A]] = totally {
    case EnvT((ts, Const(Embed(C(Str(s)))))) if s.length > maxLength.value =>
      val (cnt, len) = (size1(ts), A fromInt s.length)
      envT(some(TS.coll(cnt, some(len), some(len))), charArr(cnt))
  }

  /** Compress a union larger than `maxSize` by reducing the largest group of
    * values sharing a primary type to their shared type.
    */
  def narrowUnion[J: Order, A: Order: Field: ConvertableTo](
    maxSize: Positive
  )(implicit
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson]
  ): SSTF[J, A, SST[J, A]] => SSTF[J, A, SST[J, A]] = totally {
    case sstf @ EnvT((ts, Unioned(xs))) if xs.length > maxSize.value =>
      val grouped   = xs.list.groupBy(sst => TypeF.primary[J](sst.project.lower))
      val primaries = grouped.toList.map(_.bitraverse(ι, some)).unite

      val compressed = primaries.maximumBy(_._2.length).fold(grouped) {
        case (pt, ssts) =>
          grouped.insert(some(pt), ssts.foldMap(widenConst[J, A]).wrapNel)
      }

      compressed.foldMap(_.list) match {
        case ICons(x, ICons(y, zs)) => envT(ts, union[J, SST[J, A]](x, y, zs))
        case ICons(x, INil())       => envT(ts, x.project.lower)
        case INil()                 => sstf
      }
  }

  /** Replace encoded binary strings with `byte[]`. */
  def z85EncodedBinary[J, A](
    implicit
    A : Field[A],
    JR: Recursive.Aux[J, EJson]
  ): SSTF[J, A, SST[J, A]] => SSTF[J, A, SST[J, A]] = totally {
    case EnvT((ts, Const(Embed(EncodedBinary(size))))) =>
      // NB: Z85 uses 5 chars for every 4 bytes.
      val (cnt, len) = (size1(ts), some(A.fromBigInt(size)))
      envT(some(TS.coll(cnt, len, len)), byteArr(cnt))
  }

  ////

  private def sstMeasure[J, A] = StructuralType.measure[J, Option[TypeStat[A]]]

  private def sstConst[J, A]: Fold[SST[J, A], J] =
    project[SST[J, A], SSTF[J, A, ?]] composeIso envTIso composeLens _2 composePrism const

  private def byteArr[J, A](cnt: A): TypeF[J, SST[J, A]] =
    simpleArr(cnt, SimpleType.Byte)

  private def charArr[J, A](cnt: A): TypeF[J, SST[J, A]] =
    simpleArr(cnt, SimpleType.Char)

  private def simpleArr[J, A](cnt: A, st: SimpleType): TypeF[J, SST[J, A]] =
    arr[J, SST[J, A]](envT(some(TS.count(cnt)), simple[J, SST[J, A]](st)).embed.right)

  private def compressMap[J: Order, A: Order: Field: ConvertableTo](
    m: J ==>> SST[J, A]
  )(implicit
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson]
  ): (SST[J, A], SST[J, A]) =
    m.toList foldMap { case (j, sst) =>
      (SST.fromEJson(size1(sst.copoint), j), sst)
    }

  /** Returns the SST of the primary type of the given EJson value.
    *
    * ∀ a j. Const(j).embed <: primarySST(a, j).toType[T]
    */
  private def primarySST[J: Order, A: ConvertableTo: Field: Order](cnt: A, j: J)(
    implicit
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson]
  ): SST[J, A] = j match {
    case SimpleEJson(s)   => envT(some(TS.fromEJson(cnt, j)), simple[J, SST[J, A]](s)).embed
    case Embed(C(Str(_))) => envT(some(TS.fromEJson(cnt, j)), charArr[J, A](cnt)).embed
    case _                => SST.fromEJson(cnt, j)
  }

  private def size1[A](ots: Option[TypeStat[A]])(implicit R: Ring[A]): A =
    ots.cata(_.size, R.one)

  /** Returns the primary SST if the argument is a constant, otherwise returns
    * the argument itself.
    */
  private def widenConst[J: Order, A: ConvertableTo: Field: Order](
    sst: SST[J, A]
  )(implicit
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson]
  ): SST[J, A] =
    sstConst[J, A].headOption(sst).fold(sst) { j =>
      val ts = sstMeasure[J, A].get(sst)
      sstMeasure[J, A].set(ts)(primarySST(size1(ts), j))
    }
}

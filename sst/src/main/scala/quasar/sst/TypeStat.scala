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

package quasar.sst

import slamdata.Predef.{Byte => SByte, Char => SChar, _}
import quasar.{ejson => ejs}, ejs.{CommonEJson => C, Decoded, DecodeEJson, EJson, ExtEJson => E, EncodeEJson}
import quasar.ejson.implicits._
import quasar.fp.numeric.SampleStats
import quasar.contrib.iota.copkTraverse
import quasar.tpe.TypeF

import matryoshka._
import matryoshka.implicits._
import monocle.Prism
import scalaz._, Scalaz._, NonEmptyList.nel
import scalaz.std.anyVal.{char => charInst}
import spire.algebra.{AdditiveMonoid, AdditiveSemigroup, Field, NRoot}
import spire.math.ConvertableTo
import spire.syntax.field._

sealed abstract class TypeStat[A] {
  import TypeStat._

  /** The number of observations. */
  def size(implicit A: AdditiveSemigroup[A]): A = this match {
    case  Bool(t, f   )       => A.plus(t, f)
    case  Byte(c, _, _)       => c
    case  Char(s, _, _)       => s.size
    case   Int(s, _, _)       => s.size
    case   Dec(s, _, _)       => s.size
    case  Coll(c, _, _)       => c
    case Count(c      )       => c
  }

  /** Combine with `other` accumulating type specific information when possible
    * and summing counts otherwise.
    */
  def + (other: TypeStat[A])(implicit O: Order[A], F: Field[A]): TypeStat[A] =
    (this, other) match {
      case (Bool(t1, f1        ), Bool(t2, f2        )) => bool(t1 + t2, f1 + f2)
      case (Byte(c1, min1, max1), Byte(c2, min2, max2)) => byte(c1 + c2, mn(min1, min2), mx(max1, max2))
      case (Char(s1, min1, max1), Char(s2, min2, max2)) => char(s1 + s2, mn(min1, min2), mx(max1, max2))
      case ( Int(s1, min1, max1),  Int(s2, min2, max2)) =>  int(s1 + s2, mn(min1, min2), mx(max1, max2))
      case ( Dec(s1, min1, max1),  Dec(s2, min2, max2)) =>  dec(s1 + s2, mn(min1, min2), mx(max1, max2))
      case (Coll(c1, min1, max1), Coll(c2, min2, max2)) => coll(c1 + c2, mn(min1, min2), mx(max1, max2))
      case (                   x,                    y) => count(x.size + y.size)
    }

  // NB: Avoids conflicts between min/max and implicit widening for byte
  private def mn[A: Order](x: A, y: A): A = Order[A].min(x, y)
  private def mx[A: Order](x: A, y: A): A = Order[A].max(x, y)
}

object TypeStat extends TypeStatInstances {
  final case class  Bool[A](trues: A, falses: A)                                     extends TypeStat[A]
  final case class  Byte[A](cnt: A, min: SByte, max: SByte)                          extends TypeStat[A]
  final case class  Char[A](stats: SampleStats[A], min: SChar, max: SChar)           extends TypeStat[A]
  final case class   Int[A](stats: SampleStats[A], min: BigInt, max: BigInt)         extends TypeStat[A]
  final case class   Dec[A](stats: SampleStats[A], min: BigDecimal, max: BigDecimal) extends TypeStat[A]
  final case class  Coll[A](cnt: A, minSize: Option[A], maxSize: Option[A])          extends TypeStat[A]
  final case class Count[A](cnt: A)                                                  extends TypeStat[A]

  def bool[A] = Prism.partial[TypeStat[A], (A, A)] {
    case Bool(t, f) => (t, f)
  } ((Bool[A](_, _)).tupled)

  def byte[A] = Prism.partial[TypeStat[A], (A, SByte, SByte)] {
    case Byte(n, min, max) => (n, min, max)
  } ((Byte[A](_, _, _)).tupled)

  def char[A] = Prism.partial[TypeStat[A], (SampleStats[A], SChar, SChar)] {
    case Char(ss, min, max) => (ss, min, max)
  } ((Char[A](_, _, _)).tupled)

  def int[A] = Prism.partial[TypeStat[A], (SampleStats[A], BigInt, BigInt)] {
    case Int(ss, min, max) => (ss, min, max)
  } ((Int[A](_, _, _)).tupled)

  def dec[A] = Prism.partial[TypeStat[A], (SampleStats[A], BigDecimal, BigDecimal)] {
    case Dec(ss, min, max) => (ss, min, max)
  } ((Dec[A](_, _, _)).tupled)

  def coll[A] = Prism.partial[TypeStat[A], (A, Option[A], Option[A])] {
    case Coll(c, min, max) => (c, min, max)
  } ((Coll[A](_, _, _)).tupled)

  def count[A] = Prism.partial[TypeStat[A], A] {
    case Count(n) => n
  } (Count(_))

  def fromEJson[A, J](cnt: A, j: J)(
    implicit
    J: Recursive.Aux[J, EJson],
    M: AdditiveMonoid[A],
    A: ConvertableTo[A]
  ): TypeStat[A] = j.project match {
    case C(   ejs.Null())  => count(cnt)
    case C(   ejs.Bool(b)) => bool(b.fold(cnt, M.zero), b.fold(M.zero, cnt))
    case E(   ejs.Byte(b)) => byte(cnt, b, b)
    case E(   ejs.Char(c)) => char(SampleStats.freq(cnt, A fromInt c.toInt), c, c)
    case C(    ejs.Str(s)) => coll(cnt, some(A fromInt s.length), some(A fromInt s.length))
    case E(    ejs.Int(i)) => int(SampleStats.freq(cnt, A fromBigInt     i), i, i)
    case C(    ejs.Dec(d)) => dec(SampleStats.freq(cnt, A fromBigDecimal d), d, d)
    case C(   ejs.Arr(xs)) => fromFoldable(cnt, xs)
    case E(   ejs.Map(xs)) => fromFoldable(cnt, xs)
    case E(ejs.Meta(_, _)) => count(cnt)
  }

  def fromFoldable[F[_]: Foldable, A](cnt: A, fa: F[_])(implicit A: ConvertableTo[A]): TypeStat[A] =
    (coll(cnt, _: Option[A], _: Option[A])).tupled(some(A fromInt fa.length).squared)

  def fromTypeFƒ[J, A: Order: ConvertableTo](cnt: A)(
    implicit
    J: Recursive.Aux[J, EJson],
    F: Field[A]
  ): Algebra[TypeF[J, ?], TypeStat[A]] = {
    case TypeF.Bottom()              => count(F.zero)
    case TypeF.Top()                 => count(cnt)
    case TypeF.Simple(_)             => count(cnt)
    case TypeF.Const(j)              => fromEJson(cnt, j)
    case TypeF.Arr(-\/(xs))          => fromFoldable(maxOr(cnt, xs), xs)
    case TypeF.Arr(\/-(x))           => coll(x.size, none, none)
    case TypeF.Map(xs, None)         => fromFoldable(maxOr(cnt, xs), xs)

    case TypeF.Map(xs, Some((a, b))) =>
      val n = OneAnd(a, OneAnd(b, xs)).maximumOf1(_.size)
      coll(n, some(F fromInt xs.size), none)

    case TypeF.Union(a, b, cs)       => nel(a, b :: cs).suml1
  }

  ////

  private def maxOr[F[_]: Foldable, A: Order: AdditiveSemigroup](a: A, fa: F[TypeStat[A]]): A =
    fa.maximumOf(_.size) | a
}

sealed abstract class TypeStatInstances {
  import TypeStat._

  implicit def decodeEJson[A](implicit A: DecodeEJson[A]): DecodeEJson[TypeStat[A]] =
    new DecodeEJson[TypeStat[A]] {
      def decode[J](j: J)(implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]) = {
        def minmax[B: DecodeEJson](m: J): Decoded[(A, B, B)] =
          (m.decodedKeyS[A](CountKey) |@| m.decodedKeyS[B](MinKey) |@| m.decodedKeyS[B](MaxKey)).tupled

        def dist[B: DecodeEJson](m: J): Decoded[(SampleStats[A], B, B)] = {
          val ss = for {
            dmap <- m.decodeKeyS(DistributionKey)
            smap <- dmap.decodeKeyS(StateKey)
            size <- smap.decodedKeyS[A](SizeKey)
            m1   <- smap.decodedKeyS[A](M1Key)
            m2   <- smap.decodedKeyS[A](M2Key)
            m3   <- smap.decodedKeyS[A](M3Key)
            m4   <- smap.decodedKeyS[A](M4Key)
          } yield SampleStats(size, m1, m2, m3, m4)

          (ss |@| m.decodedKeyS[B](MinKey) |@| m.decodedKeyS[B](MaxKey)).tupled
        }

        for {
          kind <- j.decodedKeyS[String](KindKey)

          ts <- kind match {
            case Kind.Boolean =>
              (j.decodedKeyS[A](TrueKey) |@| j.decodedKeyS[A](FalseKey))(TypeStat.bool(_, _))

            case Kind.Byte =>
              minmax[SByte](j) map (TypeStat.byte(_))

            case Kind.Char =>
              dist[SChar](j) map (TypeStat.char(_))

            case Kind.Int =>
              dist[BigInt](j) map (TypeStat.int(_))

            case Kind.Dec =>
              dist[BigDecimal](j) map (TypeStat.dec(_))

            case Kind.Count =>
              j.decodedKeyS[A](CountKey) map (TypeStat.count(_))

            case Kind.Coll =>
              (
                j.decodedKeyS[A](CountKey)                |@|
                j.keyS(MinLenKey).traverse(_.decodeAs[A]) |@|
                j.keyS(MaxLenKey).traverse(_.decodeAs[A])
              )(TypeStat.coll(_, _, _))

            case other =>
              Decoded.failureFor[TypeStat[A]](j, s"Unknown TypeStat: $kind.")
          }
        } yield ts
      }
    }

  implicit def encodeEJson[A: EncodeEJson: Equal: Field: NRoot]: EncodeEJson[TypeStat[A]] =
    new EncodeEJson[TypeStat[A]] {
      def encode[J](ts: TypeStat[A])(implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): J =
        encodeEJson0(ts, isPopulation = false)
    }

  implicit def semigroup[A: Order: Field]: Semigroup[TypeStat[A]] =
    Semigroup.instance(_ + _)

  implicit def equal[A: Equal]: Equal[TypeStat[A]] =
    Equal.equal((a, b) => (a, b) match {
      case ( Bool(x1, x2    ),  Bool(y1, y2    )) => x1 ≟ y1 && x2 ≟ y2
      case ( Byte(x1, x2, x3),  Byte(y1, y2, y3)) => x1 ≟ y1 && x2 ≟ y2 && x3 ≟ y3
      case ( Char(x1, x2, x3),  Char(y1, y2, y3)) => x1 ≟ y1 && x2 ≟ y2 && x3 ≟ y3
      case (  Int(x1, x2, x3),   Int(y1, y2, y3)) => x1 ≟ y1 && x2 ≟ y2 && x3 ≟ y3
      case (  Dec(x1, x2, x3),   Dec(y1, y2, y3)) => x1 ≟ y1 && x2 ≟ y2 && x3 ≟ y3
      case ( Coll(x1, x2, x3),  Coll(y1, y2, y3)) => x1 ≟ y1 && x2 ≟ y2 && x3 ≟ y3
      case (Count(x1        ), Count(y1        )) => x1 ≟ y1

      case _ => false
    })

  implicit def show[A: Show: Equal: Field: NRoot]: Show[TypeStat[A]] =
    Show.shows {
      case  Bool(t, f       )             => s"Bool(${t.shows}, ${f.shows})"
      case  Byte(c, min, max)             => s"Byte(${c.shows}, ${min.shows}, ${max.shows})"
      case  Char(s, min, max)             => s"Char(${s.shows}, ${min.shows}, ${max.shows})"
      case   Int(s, min, max)             => s"Int(${s.shows}, ${min.shows}, ${max.shows})"
      case   Dec(s, min, max)             => s"Dec(${s.shows}, ${min.shows}, ${max.shows})"
      case  Coll(c, min, max)             => s"Coll(${c.shows}, ${min.shows}, ${max.shows})"
      case Count(c          )             => s"Count(${c.shows})"
    }

  ////

  private val CountKey = "count"
  private val DistributionKey = "distribution"
  private val FalseKey = "false"
  private val KindKey = "kind"
  private val KurtosisKey = "kurtosis"
  private val M1Key = "centralMoment1"
  private val M2Key = "centralMoment2"
  private val M3Key = "centralMoment3"
  private val M4Key = "centralMoment4"
  private val MaxKey = "max"
  private val MaxLenKey = "maxLength"
  private val MeanKey = "mean"
  private val MinKey = "min"
  private val MinLenKey = "minLength"
  private val SizeKey = "size"
  private val SkewnessKey = "skewness"
  private val StateKey = "state"
  private val TrueKey = "true"
  private val VarianceKey = "variance"

  private object Kind {
    val Boolean = "boolean"
    val Byte = "byte"
    val Char = "char"
    val Coll = "collection"
    val Count = "count"
    val Dec = "decimal"
    val Int = "integer"
  }

  private[sst] def encodeEJson0[A: EncodeEJson: Equal: Field: NRoot, J](
    ts: TypeStat[A],
    isPopulation: Boolean
  )(implicit
    JC: Corecursive.Aux[J, EJson],
    JR: Recursive.Aux[J, EJson]
  ): J = {
    def emap(xs: (String, J)*): J =
      ejs.Fixed[J].map(xs.toList.map(_.leftMap(_.asEJson[J])))

    def kmap(kind: String, rest: (String, J)*): J =
      emap((KindKey -> kind.asEJson[J]) +: rest : _*)

    def optEntry[B: EncodeEJson](k: String, b: Option[B]): List[(String, J)] =
      b.map(x => (k, x.asEJson[J])).toList

    def minmax[B: EncodeEJson](kind: String, c: A, mn: B, mx: B): J =
      kmap(
        kind
      , CountKey -> c.asEJson[J]
      , MinKey   -> mn.asEJson[J]
      , MaxKey   -> mx.asEJson[J])

    def sstats(ss: SampleStats[A]): J = {
			val state =
				emap(
					SizeKey -> ss.size.asEJson[J]
				, M1Key   -> ss.m1.asEJson[J]
				, M2Key   -> ss.m2.asEJson[J]
				, M3Key   -> ss.m3.asEJson[J]
				, M4Key   -> ss.m4.asEJson[J])

      emap(
				(StateKey, state)                                         ::
        (MeanKey, ss.mean.asEJson[J])                             ::
        optEntry(VarianceKey,
          isPopulation.fold(ss.variance, ss.populationVariance)) :::
        optEntry(SkewnessKey,
          isPopulation.fold(ss.skewness, ss.populationSkewness)) :::
        optEntry(KurtosisKey,
          isPopulation.fold(ss.kurtosis, ss.populationKurtosis)) : _*)
    }

    def dist[B: EncodeEJson](kind: String, ss: SampleStats[A], mn: B, mx: B): J =
      kmap(
        kind
      , CountKey        -> ss.size.asEJson[J]
      , DistributionKey -> sstats(ss)
      , MinKey          -> mn.asEJson[J]
      , MaxKey          -> mx.asEJson[J])

    ts match {
      case Bool(t, f) =>
        kmap(
          Kind.Boolean
        , TrueKey  -> t.asEJson[J]
        , FalseKey -> f.asEJson[J])

      case Byte(c, mn, mx) =>
        minmax(Kind.Byte, c, mn, mx)

      case Char(s, mn, mx) =>
        dist(Kind.Char, s, mn, mx)

      case Int(s, mn, mx) =>
        dist(Kind.Int, s, mn, mx)

      case Dec(s, mn, mx) =>
        dist(Kind.Dec, s, mn, mx)

      case Count(c) =>
        kmap(Kind.Count, CountKey -> c.asEJson[J])

      case Coll(c, mnl, mxl) =>
        kmap(
          Kind.Coll,
          (CountKey -> c.asEJson[J]) ::
          optEntry(MinLenKey, mnl)   :::
          optEntry(MaxLenKey, mxl) : _*)
    }
  }
}

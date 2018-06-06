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

package quasar.datagen

import slamdata.Predef._
import quasar.contrib.spire.random.dist._
import quasar.ejson.{DecodeEJson, EJson, Type => EType}
import quasar.fp.numeric.SampleStats
import quasar.fp.ski.ι
import quasar.contrib.iota.copkTraverse
import quasar.sst.{strings, Population, PopulationSST, SST, StructuralType, Tagged, TypeStat}
import quasar.tpe.TypeF

import scala.Char

import matryoshka._
import matryoshka.implicits._
import matryoshka.patterns.EnvT
import monocle.syntax.fields.{_2, _3}
import monocle.std.option.{some => someP}
import scalaz.{-\/, \/-, ==>>, Bifunctor, Equal, INil, NonEmptyList, Order, Tags}
import scalaz.Scalaz._
import spire.algebra.{AdditiveMonoid, Field, IsReal, NRoot}
import spire.math.ConvertableFrom
import spire.random.{Dist, Gaussian}
import spire.syntax.convertableFrom._
import spire.syntax.field._
import spire.syntax.isReal._
import iotaz.CopK

object dist {
  import StructuralType.{ST, STF}

  val BigIntMaxBytes     = 16
  val BigDecimalMaxScale = 34
  val StringMaxLength    = 128

  def population[J: Order, A: ConvertableFrom: Equal: Field: Gaussian: IsReal: NRoot](
      maxCollLen: A,
      src: PopulationSST[J, A])(
      implicit
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson])
      : Option[Dist[J]] =
    sst(maxCollLen, Population.unsubst(src)) { ss =>
      ss.populationStddev strengthL ss.mean
    }

  def sample[J: Order, A: ConvertableFrom: Equal: Field: Gaussian: IsReal: NRoot](
      maxCollLen: A,
      src: SST[J, A])(
      implicit
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson])
      : Option[Dist[J]] =
    sst(maxCollLen, src) { ss =>
      ss.stddev strengthL ss.mean
    }

  def sst[J: Order, A: ConvertableFrom: Equal: Field: Gaussian: IsReal](
      maxCollLen: A,
      src: SST[J, A])(
      gaussian: SampleStats[A] => Option[(A, A)])(
      implicit
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson])
      : Option[Dist[J]] =
    hylo((SST.size(src), src))(typeDistƒ[J, A](maxCollLen, gaussian), relativeProbƒ[J, A]) map (_._2)

  /** Computes the probability, relative to its parent, that a datum includes
    * the current node whilst unfolding the SST.
    */
  def relativeProbƒ[J, A: Field]: Coalgebra[STF[J, (A, TypeStat[A]), ?], (A, SST[J, A])] = {
    case (psize, sst) =>
      val size = SST.size(sst)
      Bifunctor[EnvT[?, ST[J, ?], ?]].bimap(sst.project)((size / psize, _), (size, _))
  }

  /** Returns an EJson distribution based on an SST node and the probability it
    * exists in its parent.
    */
  def typeDistƒ[J: Order, A: ConvertableFrom: Equal: Field: Gaussian: IsReal](
      maxCollLen: A,
      gaussian: SampleStats[A] => Option[(A, A)])(
      implicit
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson])
      : Algebra[STF[J, (A, TypeStat[A]), ?], Option[(A, Dist[J])]] = {
    val TF = CopK.Inject[TypeF[J, ?], ST[J, ?]]
    val TA = CopK.Inject[Tagged, ST[J, ?]]

    _.run.traverse {
      case TF(a) => \/-(a)
      case TA(a) => -\/(a)
    } map {
      case (_, TypeF.Bottom()) =>
        none

      case ((p, _), TypeF.Top()) =>
        some((p, leafDist[J]))

      case ((p, _), TypeF.Unioned(xs)) =>
        some((p, Dist.weightedMix(xs.foldMap(_.map(_.leftMap(_.toDouble)).toList) : _*)))

      case ((p, _), TypeF.Arr(-\/(INil()))) =>
        some((p, Dist.constant(EJson.arr[J]())))

      case ((p, _), TypeF.Arr(-\/(xs))) =>
        val probSpans = spansBy(xs.unite.toList)(_._1) map { dists =>
          Tags.FirstVal.unsubst[(?, NonEmptyList[Dist[J]]), A](
            dists.traverse1(Tags.FirstVal.subst[(?, Dist[J]), A](_)))
        }

        val arrDists =
          probSpans.mapAccumL(List[Dist[J]]()) { case (s0, (p, ds)) =>
            val s1 = s0 ::: ds.toList
            (s1, (p.toDouble, s1.sequence))
          }

        some((p, Dist.weightedMix(arrDists._2 : _*) map (EJson.arr(_ : _*))))

      case ((p, s), TypeF.Arr(\/-(x))) =>
        val (minl, maxl) = collBounds(s, maxCollLen).umap(_.toInt)
        x map { case (_, d) => (p, Dist.list(minl, maxl)(d) map (EJson.arr(_ : _*))) }

      case ((p, s), TypeF.Map(kn, None)) =>
        some((p, knownKeysDist(kn mapOption ι) map (EJson.map(_ : _*))))

      case ((p, s), TypeF.Map(kn, Some((k, v)))) =>
        val defkn = kn mapOption ι

        val unkct =
          (collMax getOption s getOrElse maxCollLen).toInt - defkn.size

        val unkDist =
          (k |@| v) { case ((a, kd), (_, vd)) =>
            Dist.weightedMix(
              a.toDouble -> Dist.list(1, unkct)(kd tuple vd),
              (1.0 - a.toDouble) -> Dist.constant(List[(J, J)]()))
          } getOrElse Dist.constant(List())

        some((p, (knownKeysDist(defkn) |@| unkDist)((kn, unk) => EJson.map(kn ::: unk : _*))))

      case ((p, _), TypeF.Const(j)) =>
        some((p, Dist.constant(j)))

      case ((p, s), _) =>
        typeStatDist(gaussian, s) strengthL p

    } valueOr {
      case Tagged(strings.StructuralString, dist) =>
        dist.map(_.map(_.map { j =>
          DecodeEJson[List[Char]].decode(j)
            .map(_.mkString)
            .fold((_, _) => j, EJson.str(_))
        }))

      /** TODO: Inspect Tagged values for known types (esp. temporal) for
        *       more declarative generation.
        */
      case Tagged(t, dist) =>
        dist.map(_.map(_.map(EJson.meta(_, EType(t)))))
    }
  }
  ////

  private def clamp[A: Order](min: A, max: A): A => A =
    _.min(max).max(min)

  private def collBounds[A: AdditiveMonoid](ts: TypeStat[A], maxLen: A): (A, A) =
    (
      collMin[A] getOption ts getOrElse AdditiveMonoid[A].zero,
      collMax[A] getOption ts getOrElse maxLen
    )

  private def collMin[A] = TypeStat.coll[A] composeLens _2 composePrism someP
  private def collMax[A] = TypeStat.coll[A] composeLens _3 composePrism someP

  private def knownKeysDist[J, A: ConvertableFrom](kn: J ==>> (A, Dist[J])): Dist[List[(J, J)]] =
    kn.toList traverse { case (k, (a, v)) =>
      val p = a.toDouble

      Dist.weightedMix[List[(J, J)]](
        (p, v map (j => List((k, j)))),
        (1.0 - p, Dist.constant(List())))
    } map (_.join)

  private def leafDist[J](implicit J: Corecursive.Aux[J, EJson]): Dist[J] =
    Dist.mix(
      Dist[Boolean] map (EJson.bool(_)),
      Dist[Byte] map (EJson.byte(_)),
      Dist[Char] map (EJson.char(_)),
      Dist.bigdecimal(BigIntMaxBytes, BigDecimalMaxScale) map (EJson.dec(_)),
      Dist.bigint(BigIntMaxBytes) map (EJson.int(_)),
      Dist.constant(EJson.nul[J]),
      Dist.list[Char](0, StringMaxLength) map (cs => EJson.str(cs.mkString)))

  /** Returns a list of spans having the same `B`. Preserves order of the
    * original list such that `spansBy(xs)(f).flatMap(_.toList) === xs`.
    */
  private def spansBy[A, B: Equal](as: List[A])(f: A => B): List[NonEmptyList[A]] = {
    @tailrec
    def loop(xs: List[A], ss: List[NonEmptyList[A]]): List[NonEmptyList[A]] =
      xs match {
        case Nil => ss

        case h :: t =>
          val b = f(h)
          val (s, ys) = t.span(a => f(a) ≟ b)
          loop(ys, NonEmptyList.nels(h, s : _*) :: ss)
      }

    loop(as, Nil).reverse
  }

  /** Attempt to build a `Dist` from a `TypeStat`, given a function to
    * extract the mean and stddev from `SampleStats`.
    */
  private def typeStatDist[J, A: ConvertableFrom: Field: Gaussian: IsReal](
      gaussian: SampleStats[A] => Option[(A, A)],
      stat: TypeStat[A])(
      implicit
      J: Corecursive.Aux[J, EJson])
      : Option[Dist[J]] =
    some(stat) collect {
      case TypeStat.Bool(ts, fs) =>
        val total = ts + fs
        Dist.weightedMix(
          ((ts / total).toDouble, Dist.constant(true)),
          ((fs / total).toDouble, Dist.constant(false))
        ) map (EJson.bool(_))

      case TypeStat.Byte(_, bn, bx) =>
        Dist.intrange(bn.toInt, bx.toInt) map (i => EJson.byte(i.toByte))

      case TypeStat.Char(ss, cn, cx) =>
        gaussian(ss)
          .cata((Dist.gaussian[A] _).tupled, Dist.constant(ss.mean))
          .map((EJson.char[J](_)) <<< ((_: Int).toChar) <<< clamp(cn.toInt, cx.toInt) <<< ((_: A).round.toInt))

      case TypeStat.Int(ss, mn, mx) =>
        gaussian(ss)
          .cata((Dist.gaussian[A] _).tupled, Dist.constant(ss.mean))
          .map((EJson.int[J](_)) <<< clamp(mn, mx) <<< ((_: A).toBigInt))

      case TypeStat.Dec(ss, mn, mx) =>
        gaussian(ss)
          .cata((Dist.gaussian[A] _).tupled, Dist.constant(ss.mean))
          .map((EJson.dec[J](_)) <<< clamp(mn, mx) <<< ((_: A).toBigDecimal))
    }
}

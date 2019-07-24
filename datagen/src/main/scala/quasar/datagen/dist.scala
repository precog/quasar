/*
 * Copyright 2014–2019 SlamData Inc.
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
import quasar.common.data.Data.DateTimeConstants
import quasar.contrib.spire.random.dist._
import quasar.ejson.{DecodeEJson, EJson, Fixed => EFixed, Type => EType, TypeTag}
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
import scalaz.{-\/, \/-, ==>>, Bifunctor, Equal, IList, INil, NonEmptyList, Order, Tags}
import scalaz.Scalaz._
import scalaz.syntax.tag._
import spire.algebra.{Field, IsReal, NRoot}
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

      case ((p, _), TypeF.Arr(INil(), None)) =>
        some((p, Dist.constant(EJson.arr[J]())))

      case ((p, s), TypeF.Arr(INil(), Some(x))) =>
        val (minl, maxl) = collBounds(s, maxCollLen.toInt)
        x map { case (_, d) => (p, Dist.list(minl, maxl)(d) map (EJson.arr(_ : _*))) }

      case ((p, _), TypeF.Arr(xs, None)) =>
        some((p, knownElementsDist(xs) map (EJson.arr(_ : _*))))

      case ((p, s), TypeF.Arr(xs, Some(ux))) =>
        val maxKnown = xs.length

        val unkDist = ux traverse { case (a, d) =>
          val cnt =
            Order[Int].max(
              1,
              (collMax.getOption(s) getOrElse maxCollLen).toInt - maxKnown)

          Dist.weightedMix(
            a.toDouble -> Dist.list(1, cnt)(d),
            (1.0 - a.toDouble) -> Dist.constant(List.empty[J]))
        }

        val arrDist = for {
          kn <- knownElementsDist(xs)

          uk <-
            if (kn.length === maxKnown) unkDist
            else Dist.constant(None)

        } yield kn ::: uk.getOrElse(List.empty[J])

        some((p, arrDist map (EJson.arr(_ : _*))))

      case ((p, s), TypeF.Map(kn, None)) =>
        some((p, knownKeysDist(kn mapOption ι) map (EJson.map(_ : _*))))

      case ((p, s), TypeF.Map(kn, Some((k, v)))) =>
        val defkn = kn mapOption ι

        val unkct =
          Order[Int].max(
            1,
            (collMax getOption s getOrElse maxCollLen).toInt - defkn.size)

        val unkDist =
          (k |@| v) { case ((a, kd), (_, vd)) =>
            Dist.weightedMix(
              a.toDouble -> Dist.list(1, unkct)(kd tuple vd),
              (1.0 - a.toDouble) -> Dist.constant(List.empty[(J, J)]))
          } getOrElse Dist.constant(List.empty[(J, J)])

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

      case Tagged(tt @ TypeTag.OffsetDateTime, dist) =>
        dist.map(_.map(_.map(j => EJson.meta(fixupMonths(j), EType(tt)))))

      case Tagged(tt @ TypeTag.OffsetDate, dist) =>
        dist.map(_.map(_.map(j => EJson.meta(fixupMonths(j), EType(tt)))))

      case Tagged(tt @ TypeTag.LocalDateTime, dist) =>
        dist.map(_.map(_.map(j => EJson.meta(fixupMonths(j), EType(tt)))))

      case Tagged(tt @ TypeTag.LocalDate, dist) =>
        dist.map(_.map(_.map(j => EJson.meta(fixupMonths(j), EType(tt)))))

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

  private def collBounds[A: ConvertableFrom](ts: TypeStat[A], maxLen: Int): (Int, Int) =
    (
      collMin[A].getOption(ts).fold(0)(x => Order[Int].min(x.toInt, maxLen)),
      collMax[A].getOption(ts).fold(maxLen)(_.toInt)
    )

  private def collMin[A] = TypeStat.coll[A] composeLens _2 composePrism someP
  private def collMax[A] = TypeStat.coll[A] composeLens _3 composePrism someP

  private def fixupMonths[J: Equal](
      j: J)(
      implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson])
      : J = {

    val J = EFixed[J]

    val fixup = for {
      xs <- J.map.getOption(j)

      (_, v) <- xs.find(_._1 === J.str(DateTimeConstants.month))

      i <- J.int.getOption(v)

      ys = xs map {
        case (k @ J.str(DateTimeConstants.day), J.int(d)) =>
          val clampd = i.toInt match {
            case 2 => d.min(28)
            case 4 | 6 | 9 | 11 => d.min(30)
            case _ => d
          }

          (k, J.int(clampd))

        case other => other
      }
    } yield J.map(ys)

    fixup getOrElse j
  }

  private def knownElementsDist[J, A: ConvertableFrom: Equal](elts: IList[Option[(A, Dist[J])]]): Dist[List[J]] = {
    val probSpans = spansBy(elts.unite.toList)(_._1) map { dists =>
      Tags.FirstVal.unsubst[(?, NonEmptyList[Dist[J]]), A](
        dists.traverse1(Tags.FirstVal.subst[(?, Dist[J]), A](_)))
    }

    val knownDists =
      probSpans.mapAccumL(List[Dist[J]]()) { case (s0, (p, ds)) =>
        val s1 = s0 ::: ds.toList
        (s1, (p.toDouble, s1.sequence))
      }

    Dist.weightedMix(knownDists._2 : _*)
  }

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
    stat match {
      case TypeStat.Bool(ts, fs) =>
        val total = ts + fs
        some(Dist.weightedMix(
          ((ts / total).toDouble, Dist.constant(true)),
          ((fs / total).toDouble, Dist.constant(false))).map(EJson.bool(_)))

      case TypeStat.Char(ss, cn, cx) =>
        some(gaussian(ss)
          .cata((Dist.gaussian[A] _).tupled, Dist.constant(ss.mean))
          .map((EJson.char[J](_)) <<< ((_: Int).toChar) <<< clamp(cn.toInt, cx.toInt) <<< ((_: A).round.toInt)))

      case TypeStat.Str(c, mnl, mxl, mn, mx) =>
        val minmax =
          (mn + mx).toList.foldMap(c => some((Tags.MinVal(c), Tags.MaxVal(c))))

        val (minc, maxc) =
          minmax.fold((Char.MinValue, Char.MaxValue))(_.bimap(_.unwrap, _.unwrap))

        val charRange =
          Dist.intrange(minc.toInt, maxc.toInt) map (_.toChar)

        some(Dist.list(mnl.toInt, mxl.toInt)(charRange) map (cs => EJson.str(cs.mkString)))

      case TypeStat.Int(ss, mn, mx) =>
        some(gaussian(ss)
          .cata((Dist.gaussian[A] _).tupled, Dist.constant(ss.mean))
          .map((EJson.int[J](_)) <<< clamp(mn, mx) <<< ((_: A).toBigInt)))

      case TypeStat.Dec(ss, mn, mx) =>
        some(gaussian(ss)
          .cata((Dist.gaussian[A] _).tupled, Dist.constant(ss.mean))
          .map((EJson.dec[J](_)) <<< clamp(mn, mx) <<< ((_: A).toBigDecimal)))

      case TypeStat.Coll(_, _, _) => none
      case TypeStat.Count(_) => none
    }
}

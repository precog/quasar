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

package quasar.impl.schema

import slamdata.Predef._

import quasar.contrib.iota._
import quasar.contrib.matryoshka.envT
import quasar.ejson.{EJson, TemporalKeys, TypeTag}
import quasar.fp.numeric.SampleStats
import quasar.sst._
import quasar.tpe.{TypeF, SimpleType}

import java.math.MathContext
import java.time.{
  LocalDate,
  LocalDateTime,
  LocalTime,
  OffsetDateTime,
  OffsetTime
}

import matryoshka.{Corecursive, Recursive}
import matryoshka.implicits._
import qdata.QDataEncode
import qdata.time.{DateTimeInterval, OffsetDate}
import scalaz.{IList, IMap, Order}
import scalaz.std.anyVal._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.string._
import scalaz.std.tuple._
import scalaz.syntax.bifunctor._
import scalaz.syntax.equal._
import scalaz.syntax.foldable._
import scalaz.syntax.semigroup._
import spire.algebra.Field
import spire.math.{ConvertableTo, Real}

object QDataCompressedSst {
  import StructuralType.{TagST, TypeST}

  def encode[J: Order, A: Order](config: SstConfig[J, A])(
      implicit
      AF: Field[A],
      AC: ConvertableTo[A],
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson])
      : QDataEncode[SST[J, A]] =
    new QDataEncode[SST[J, A]] {

      private val boolType = TypeST(TypeF.simple[J, SST[J, A]](SimpleType.Bool))
      private val intType = TypeST(TypeF.simple[J, SST[J, A]](SimpleType.Int))
      private val decType = TypeST(TypeF.simple[J, SST[J, A]](SimpleType.Dec))
      private val strType = TypeST(TypeF.simple[J, SST[J, A]](SimpleType.Str))

      private val trueStat = TypeStat.bool(AF.one, AF.zero)
      private val falseStat = TypeStat.bool(AF.zero, AF.one)

      private val maxStr = config.stringMaxLength.value.toInt
      private val maxArr = config.arrayMaxLength.value
      private val retIdx = config.retainIndicesSize.value.toInt
      private val maxMap = config.mapMaxSize.value.toInt
      private val retKeys = config.retainKeysSize.value.toInt

      def makeLong(l: Long): SST[J, A] =
        envT(
          TypeStat.int(SampleStats.one(AC.fromLong(l)), BigInt(l), BigInt(l)),
          intType).embed

      def makeDouble(l: Double): SST[J, A] =
        envT(
          TypeStat.dec(SampleStats.one(AC.fromDouble(l)), BigDecimal(l), BigDecimal(l)),
          decType).embed

      def makeReal(l: Real): SST[J, A] =
        if (l.isWhole) {
          val i = l.toRational.toBigInt
          envT(TypeStat.int(SampleStats.one(AC.fromReal(l)), i, i), intType).embed
        } else {
          val d = l.toRational.toBigDecimal(MathContext.UNLIMITED)
          envT(TypeStat.dec(SampleStats.one(AC.fromReal(l)), d, d), decType).embed
        }

      def makeString(l: String): SST[J, A] = {
        val stat =
          TypeStat.str(AF.one, AC.fromInt(l.length), AC.fromInt(l.length), l, l)

        if (config.stringPreserveStructure && l.length <= maxStr)
          strings.widenStats[J, A](stat, l).embed
        else
          envT(stat, strType).embed
      }

      val makeNull: SST[J, A] =
        envT(
          TypeStat.count(AF.one),
          TypeST(TypeF.simple[J, SST[J, A]](SimpleType.Null))).embed

      def makeBoolean(l: Boolean): SST[J, A] =
        envT(if (l) trueStat else falseStat, boolType).embed

      def makeLocalDateTime(l: LocalDateTime): SST[J, A] =
        temporalMapI(
          TypeTag.LocalDateTime,
          TemporalKeys.year -> l.getYear,
          TemporalKeys.month -> l.getMonth.getValue,
          TemporalKeys.day -> l.getDayOfMonth,
          TemporalKeys.hour -> l.getHour,
          TemporalKeys.minute -> l.getMinute,
          TemporalKeys.second -> l.getSecond,
          TemporalKeys.nanosecond -> l.getNano)

      def makeLocalDate(l: LocalDate): SST[J, A] =
        temporalMapI(
          TypeTag.LocalDate,
          TemporalKeys.year -> l.getYear,
          TemporalKeys.month -> l.getMonth.getValue,
          TemporalKeys.day -> l.getDayOfMonth)

      def makeLocalTime(l: LocalTime): SST[J, A] =
        temporalMapI(
          TypeTag.LocalTime,
          TemporalKeys.hour -> l.getHour,
          TemporalKeys.minute -> l.getMinute,
          TemporalKeys.second -> l.getSecond,
          TemporalKeys.nanosecond -> l.getNano)

      def makeOffsetDateTime(l: OffsetDateTime): SST[J, A] =
        temporalMapI(
          TypeTag.OffsetDateTime,
          TemporalKeys.year -> l.getYear,
          TemporalKeys.month -> l.getMonth.getValue,
          TemporalKeys.day -> l.getDayOfMonth,
          TemporalKeys.hour -> l.getHour,
          TemporalKeys.minute -> l.getMinute,
          TemporalKeys.second -> l.getSecond,
          TemporalKeys.nanosecond -> l.getNano,
          TemporalKeys.offset -> l.getOffset.getTotalSeconds)

      def makeOffsetDate(l: OffsetDate): SST[J, A] =
        temporalMapI(
          TypeTag.OffsetDate,
          TemporalKeys.year -> l.date.getYear,
          TemporalKeys.month -> l.date.getMonth.getValue,
          TemporalKeys.day -> l.date.getDayOfMonth,
          TemporalKeys.offset -> l.offset.getTotalSeconds)

      def makeOffsetTime(l: OffsetTime): SST[J, A] =
        temporalMapI(
          TypeTag.OffsetTime,
          TemporalKeys.hour -> l.getHour,
          TemporalKeys.minute -> l.getMinute,
          TemporalKeys.second -> l.getSecond,
          TemporalKeys.nanosecond -> l.getNano,
          TemporalKeys.offset -> l.getOffset.getTotalSeconds)

      def makeInterval(l: DateTimeInterval): SST[J, A] =
        temporalMap(
          TypeTag.Interval,
          TemporalKeys.year -> makeInt(l.period.getYears),
          TemporalKeys.month -> makeInt(l.period.getMonths),
          TemporalKeys.day -> makeInt(l.period.getDays),
          TemporalKeys.second -> makeLong(l.duration.getSeconds),
          TemporalKeys.nanosecond -> makeInt(l.duration.getNano))

      // (length, retained indices, additional indices), corresponds to TypeF.Arr
      type NascentArray = (Long, IList[SST[J, A]], Option[SST[J, A]])

      val prepArray: NascentArray = (0L, IList.empty, none)

      def pushArray(a: SST[J, A], na: NascentArray): NascentArray =
        na match {
          case (c, xs, _) if c === maxArr =>
            // xs is maintained in reverse order, so retain from the end
            val (comp, keep) = xs.splitAt(maxArr.toInt - retIdx)
            (c + 1, keep, (a :: comp).foldMap1Opt(x => x))

          case (c, xs, Some(x)) =>
            (c + 1, xs, Some(x |+| a))

          case (c, xs, x) =>
            (c + 1, a :: xs, x)
        }

      def makeArray(na: NascentArray): SST[J, A] = {
        val sz = some(AC.fromLong(na._1))
        envT(
          TypeStat.coll(AF.one, sz, sz),
          TypeST(TypeF.arr[J, SST[J, A]](na._2.reverse, na._3))).embed
      }

      // (retained entries, additional entries), corresponds to TypeF.Map
      type NascentObject = (IMap[String, SST[J, A]], Option[(SST[J, A], SST[J, A])])

      val prepObject: NascentObject = (IMap.empty, none)

      def pushObject(key: String, a: SST[J, A], na: NascentObject): NascentObject =
        na match {
          case (m, None) if m.size === maxMap =>
            val i = (m.size min retKeys) - 1
            val m1 = m.insert(key, a)
            m1.elemAt(i).fold(prepObject rightAs compressMap(m1)) {
              case (k, v) =>
                val (keep, comp) = m1.split(k)
                (keep.insert(k, v), compressMap(comp))
            }

          case (m, None) =>
            (m.insert(key, a), None)

          case (m, Some((k, v))) =>
            (m, Some((k |+| makeString(key), v |+| a)))
        }

      def makeObject(na: NascentObject): SST[J, A] =
        na match {
          case (m, unk) =>
            val kn = AC.fromInt(m.size)
            val sz = some(unk.fold(kn) { case (k, _) => AF.plus(kn, SST.size(k)) })

            envT(
              TypeStat.coll(AF.one, sz, sz),
              TypeST(TypeF.map(m.mapKeys(EJson.str[J]), unk))).embed
        }

      def makeMeta(value: SST[J, A], meta: SST[J, A]): SST[J, A] =
        value

      ////

      private def compressMap(m: IMap[String, SST[J, A]]): Option[(SST[J, A], SST[J, A])] =
        m.toList foldMap1Opt { case (k, v) => (makeString(k), v) }

      private def makeInt(i: Int): SST[J, A] =
        envT(
          TypeStat.int(SampleStats.one(AC.fromInt(i)), BigInt(i), BigInt(i)),
          intType).embed

      private def temporalMapI(tt: TypeTag, parts: (String, Int)*): SST[J, A] =
        temporalMap(tt, parts map {
          case (n, v) => (n, makeInt(v))
        }: _*)

      private def temporalMap(tt: TypeTag, parts: (String, SST[J, A])*): SST[J, A] = {
        val m = parts.foldLeft(IMap.empty[J, SST[J, A]]) {
          case (m, (n, v)) => m.insert(EJson.str(n), v)
        }
        val sz = some(AC.fromInt(m.size))
        val st = TypeStat.coll(AF.one, sz, sz)

        envT(st, TagST[J](Tagged(tt,
          envT(st, TypeST(TypeF.map(m, none))).embed))).embed
      }
    }
}

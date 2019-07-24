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

package quasar.sst

import slamdata.Predef.{Boolean, Double, Long, String}

import quasar.contrib.iota._
import quasar.contrib.matryoshka.envT
import quasar.ejson.EJson
import quasar.ejson.implicits._
import quasar.tpe.TypeF

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
import spire.algebra.Field
import spire.math.{ConvertableTo, Real}
import scalaz.{IList, IMap, Order}
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.syntax.bifunctor._

private[sst] object QDataSst {
  def encode[J: Order, A: Order](
      implicit
      AF: Field[A],
      AC: ConvertableTo[A],
      JC: Corecursive.Aux[J, EJson],
      JR: Recursive.Aux[J, EJson])
      : QDataEncode[SST[J, A]] =
    new QDataEncode[SST[J, A]] {
      val J = QDataEncode[J]

      def makeLong(l: Long): SST[J, A] =
        SST.fromEJson(AF.one, J.makeLong(l))

      def makeDouble(l: Double): SST[J, A] =
        SST.fromEJson(AF.one, J.makeDouble(l))

      def makeReal(l: Real): SST[J, A] =
        SST.fromEJson(AF.one, J.makeReal(l))

      def makeString(l: String): SST[J, A] =
        SST.fromEJson(AF.one, J.makeString(l))

      val makeNull: SST[J, A] =
        SST.fromEJson(AF.one, J.makeNull)

      def makeBoolean(l: Boolean): SST[J, A] =
        SST.fromEJson(AF.one, J.makeBoolean(l))

      def makeLocalDateTime(l: LocalDateTime): SST[J, A] =
        SST.fromEJson(AF.one, J.makeLocalDateTime(l))

      def makeLocalDate(l: LocalDate): SST[J, A] =
        SST.fromEJson(AF.one, J.makeLocalDate(l))

      def makeLocalTime(l: LocalTime): SST[J, A] =
        SST.fromEJson(AF.one, J.makeLocalTime(l))

      def makeOffsetDateTime(l: OffsetDateTime): SST[J, A] =
        SST.fromEJson(AF.one, J.makeOffsetDateTime(l))

      def makeOffsetDate(l: OffsetDate): SST[J, A] =
        SST.fromEJson(AF.one, J.makeOffsetDate(l))

      def makeOffsetTime(l: OffsetTime): SST[J, A] =
        SST.fromEJson(AF.one, J.makeOffsetTime(l))

      def makeInterval(l: DateTimeInterval): SST[J, A] =
        SST.fromEJson(AF.one, J.makeInterval(l))

      type NascentArray = (A, IList[SST[J, A]])

      val prepArray: NascentArray = (AF.zero, IList.empty)

      def pushArray(a: SST[J, A], na: NascentArray): NascentArray =
        na.bimap(AF.plus(AF.one, _), a :: _)

      def makeArray(na: NascentArray): SST[J, A] =
        envT(
          TypeStat.coll(AF.one, some(na._1), some(na._1)),
          StructuralType.TypeST(TypeF.arr[J, SST[J, A]](na._2.reverse, none[SST[J, A]]))).embed

      type NascentObject = IMap[J, SST[J, A]]

      val prepObject: NascentObject = IMap.empty

      def pushObject(key: String, a: SST[J, A], na: NascentObject): NascentObject =
        na.insert(J.makeString(key), a)

      def makeObject(na: NascentObject): SST[J, A] =
        envT(
          TypeStat.coll(AF.one, some(AC.fromInt(na.size)), some(AC.fromInt(na.size))),
          StructuralType.TypeST(TypeF.map(na, none[(SST[J, A], SST[J, A])]))).embed

      def makeMeta(value: SST[J, A], meta: SST[J, A]): SST[J, A] =
        value
    }
}

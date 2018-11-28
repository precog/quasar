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

package quasar.impl.schema

import slamdata.Predef._

import quasar.Qspec
import quasar.common.data.{RValue, RValueGenerators}
import quasar.contrib.algebra._
import quasar.contrib.iota._
import quasar.contrib.matryoshka.envT
import quasar.ejson.{EJson, Fixed}
import quasar.ejson.implicits._
import quasar.fp.numeric.SampleStats
import quasar.sst.{sstQDataEncode => _, _}
import quasar.sst.StructuralType.TypeST
import quasar.tpe._

import java.math.MathContext

import scala.Predef.implicitly

import eu.timepit.refined.auto._
import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import matryoshka.patterns.EnvT
import qdata.QData
import scalaz.{@@, IList, IMap, NonEmptyList, Show}
import scalaz.Tags.Disjunction
import scalaz.std.anyVal._
import scalaz.std.anyVal._
import scalaz.syntax.foldable1._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._
import spire.math.Real

object QDataCompressedSstSpec extends Qspec with RValueGenerators {

  type J = Fix[EJson]
  type S = SST[J, Real]

  val J = Fixed[J]

  implicit val realShow: Show[Real] =
    Show.showFromToString

  val config =
    SstConfig.Default.copy[J, Real](retainKeysSize = 5L, retainIndicesSize = 5L)

  val structConfig =
    config.copy[J, Real](stringPreserveStructure = true)

  val containLiterals =
    beTrue ^^ { (s: S) =>
      Disjunction.unwrap(s.cata[Boolean @@ Disjunction] {
        case EnvT((_, TypeST(TypeF.Const(_)))) => true.disjunction
        case other => other.fold
      })
    }

  def ints(n: Int, ns: Int*): NonEmptyList[S] =
    NonEmptyList(n, ns: _*) map { n =>
      envT(
        TypeStat.int(SampleStats.one(Real(n)), BigInt(n), BigInt(n)),
        TypeST(TypeF.simple[J, S](SimpleType.Int))).embed
    }

  def convert(cfg: SstConfig[J, Real], rv: RValue): S =
    QData.convert(rv)(implicitly, QDataCompressedSst.encode(cfg))

  "produces no literals" >> prop { rv: RValue =>
    convert(config, rv) must not(containLiterals)
  }

  "produces no literals when preserving string structure" >> prop { rv: RValue =>
    convert(structConfig, rv) must not(containLiterals)
  }

  "interprets integral Reals as Int" >> prop { l: Long =>
    val r = Real(l)
    val i = r.toRational.toBigInt

    val exp =
      envT(
        TypeStat.int(SampleStats.one(r), i, i),
        TypeST(TypeF.simple[J, S](SimpleType.Int))).embed

    QDataCompressedSst.encode(config).makeReal(r) must equal(exp)
  }

  "interprets non-integral Reals as Dec" >> {
    val r = Real.pi
    val b = r.toRational.toBigDecimal(MathContext.UNLIMITED)

    val exp =
      envT(
        TypeStat.dec(SampleStats.one(r), b, b),
        TypeST(TypeF.simple[J, S](SimpleType.Dec))).embed

    QDataCompressedSst.encode(config).makeReal(r) must equal(exp)
  }

  "compresses maps larger than max size" >> {
    val input = RValue.rObject(Map(
      "foo" -> RValue.rLong(1),
      "bar" -> RValue.rLong(1),
      "baz" -> RValue.rLong(1),
      "quux" -> RValue.rLong(1)))

    val expected = envT(
      TypeStat.coll(Real(1), Real(4).some, Real(4).some),
      TypeST(TypeF.map[J, S](IMap.empty[J, S], Some((
        envT(
          TypeStat.str(Real(4), Real(3), Real(4), "bar", "quux"),
          TypeST(TypeF.simple[J, S](SimpleType.Str))).embed,
        envT(
          TypeStat.int(SampleStats.freq(Real(4), Real(1)), BigInt(1), BigInt(1)),
          TypeST(TypeF.simple[J, S](SimpleType.Int))).embed
      ))))).embed

    convert(config.copy(mapMaxSize = 3L, retainKeysSize = 0L), input) must_= expected
  }

  "retains k map keys" >> {
    val input = RValue.rObject(Map(
      "foo" -> RValue.rLong(1),
      "bar" -> RValue.rLong(1),
      "baz" -> RValue.rLong(1),
      "quux" -> RValue.rLong(1)))

    val expected = envT(
      TypeStat.coll(Real(1), Real(4).some, Real(4).some),
      TypeST(TypeF.map[J, S](
        IMap(
          J.str("bar") -> ints(1).head,
          J.str("baz") -> ints(1).head),
        Some((
          envT(
            TypeStat.str(Real(2), Real(3), Real(4), "foo", "quux"),
            TypeST(TypeF.simple[J, S](SimpleType.Str))).embed,
          envT(
            TypeStat.int(SampleStats.freq(Real(2), Real(1)), BigInt(1), BigInt(1)),
            TypeST(TypeF.simple[J, S](SimpleType.Int))).embed))
      ))).embed

    convert(config.copy(mapMaxSize = 3L, retainKeysSize = 2L), input) must_= expected
  }

  "compresses arrays larger than max size" >> {
    val input = RValue.rObject(Map(
      "foo" -> RValue.rArray(List(RValue.rLong(1), RValue.rLong(2))),
      "bar" -> RValue.rArray(List(RValue.rLong(1), RValue.rLong(2), RValue.rLong(3)))))

    val expected = envT(
      TypeStat.coll(Real(1), Real(2).some, Real(2).some),
      TypeST(TypeF.map[J, S](IMap(
        J.str("foo") -> envT(
          TypeStat.coll(Real(1), Real(2).some, Real(2).some),
          TypeST(TypeF.arr[J, S](ints(1, 2).list, None))).embed,

        J.str("bar") -> envT(
          TypeStat.coll(Real(1), Real(3).some, Real(3).some),
          TypeST(TypeF.arr[J, S](IList[S](), Some(ints(1, 2, 3).suml1)))).embed
      ), None))).embed

    convert(config.copy(arrayMaxLength = 2L, retainIndicesSize = 0L), input) must_= expected
  }

  "retains k indices" >> {
    val input = RValue.rObject(Map(
      "foo" -> RValue.rArray(List(RValue.rLong(1), RValue.rLong(2))),
      "bar" -> RValue.rArray(List(RValue.rLong(1), RValue.rLong(2), RValue.rLong(3)))))

    val expected = envT(
      TypeStat.coll(Real(1), Real(2).some, Real(2).some),
      TypeST(TypeF.map[J, S](IMap(
        J.str("foo") -> envT(
          TypeStat.coll(Real(1), Real(2).some, Real(2).some),
          TypeST(TypeF.arr[J, S](ints(1, 2).list, None))).embed,

        J.str("bar") -> envT(
          TypeStat.coll(Real(1), Real(3).some, Real(3).some),
          TypeST(TypeF.arr[J, S](ints(1).list, Some(ints(2, 3).suml1)))).embed
      ), None))).embed

    convert(config.copy(arrayMaxLength = 2L, retainIndicesSize = 1L), input) must_= expected
  }

  "preserve structure of strings up to max length" >> {
    val input = RValue.rString("abcdef")

    val expected =
      strings.widenStats[J, Real](
        TypeStat.str(Real(1), Real(6), Real(6), "abcdef", "abcdef"),
        "abcdef").embed

    convert(config.copy(stringMaxLength = 6L, stringPreserveStructure = true), input) must_= expected
  }

  "forget structure of strings longer than max length" >> {
    val input = RValue.rString("abcdef")

    val expected =
      envT(
        TypeStat.str(Real(1), Real(6), Real(6), "abcdef", "abcdef"),
        TypeST(TypeF.simple[J, S](SimpleType.Str))).embed

    convert(config.copy(stringMaxLength = 5L, stringPreserveStructure = true), input) must_= expected
  }
}

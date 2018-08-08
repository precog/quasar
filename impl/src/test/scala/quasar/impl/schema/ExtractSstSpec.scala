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

import slamdata.Predef.{Int => SInt, Stream => _, _}
import quasar.common.data.Data
import quasar.contrib.algebra._
import quasar.contrib.iota._
import quasar.contrib.matryoshka._
import quasar.ejson.{EJson, Fixed}
import quasar.ejson.implicits._
import quasar.fp._
import quasar.fp.numeric.SampleStats
import quasar.sst._
import quasar.tpe._

import eu.timepit.refined.auto._
import fs2.Stream
import matryoshka.data.Fix
import matryoshka.implicits._
import scalaz._, Scalaz._
import spire.math.Real

final class ExtractSstSpec extends quasar.Qspec {
  import Data._, StructuralType.{TagST, TypeST}

  type J = Fix[EJson]
  type S = SST[J, Real]

  implicit val showReal: Show[Real] =
    Show.showFromToString

  val J = Fixed[J]
  val config = SstConfig[J, Real](1000L, 1000L, 1000L, 1000L, 1000L)

  def verify(cfg: SstConfig[J, Real], input: List[Data], expected: S) =
    Stream.emits(input)
      .through(extractSst(cfg))
      .covary[cats.effect.IO]
      .compile.last.unsafeRunSync
      .must(beSome(equal(expected)))

  def ints(n: SInt, ns: SInt*): NonEmptyList[S] =
    NonEmptyList(n, ns: _*) map (n =>
      envT(
        TypeStat.int(SampleStats.one(Real(n)), BigInt(n), BigInt(n)),
        TypeST(TypeF.const[J, S](J.int(BigInt(n))))).embed)

  def strSS(s: String): SampleStats[Real] =
    SampleStats.fromFoldable(s.map(c => Real(c.toInt)).toList)

  "compress arrays" >> {
    val input = List(
      _obj(ListMap("foo" -> _arr(List(_int(1), _int(2))))),
      _obj(ListMap("bar" -> _arr(List(_int(1), _int(2), _int(3)))))
    )

    val expected = envT(
      TypeStat.coll(Real(2), Real(1).some, Real(1).some),
      TypeST(TypeF.map[J, S](IMap(
        J.str("foo") -> envT(
          TypeStat.coll(Real(1), Real(2).some, Real(2).some),
          TypeST(TypeF.arr[J, S](ints(1, 2).list.left))).embed,

        J.str("bar") -> envT(
          TypeStat.coll(Real(1), Real(3).some, Real(3).some),
          TypeST(TypeF.arr[J, S](ints(1, 2, 3).suml1.right))).embed
      ), None))).embed

    verify(config.copy(arrayMaxLength = 2L), input, expected)
  }

  "compress long strings" >> {
    val input = List(
      _obj(ListMap("foo" -> _str("abcdef"))),
      _obj(ListMap("bar" -> _str("abcde")))
    )

    val expected = envT(
      TypeStat.coll(Real(2), Real(1).some, Real(1).some),
      TypeST(TypeF.map[J, S](IMap(
        J.str("foo") -> envT(
          TypeStat.coll(Real(1), Real(6).some, Real(6).some),
          TagST[J](Tagged(
            strings.StructuralString,
            envT(
              TypeStat.coll(Real(1), Real(6).some, Real(6).some),
              TypeST(TypeF.arr[J, S](envT(
                TypeStat.char(strSS("abcdef"), 'a', 'f'),
                TypeST(TypeF.simple[J, S](SimpleType.Char))).embed.right))
            ).embed))
          ).embed,

        J.str("bar") -> envT(
          TypeStat.coll(Real(1), Real(5).some, Real(5).some),
          TypeST(TypeF.const[J, S](J.str("abcde")))
        ).embed
      ), None))).embed

    verify(config.copy(stringMaxLength = 5L), input, expected)
  }

  "coalesce map keys until <= max size" >> {
    val input = List(
      _obj(ListMap("foo" -> _int(1))),
      _obj(ListMap("bar" -> _int(1))),
      _obj(ListMap("baz" -> _int(1))),
      _obj(ListMap("quux" -> _int(1)))
    )

    val expected = envT(
      TypeStat.coll(Real(4), Real(1).some, Real(1).some),
      TypeST(TypeF.map[J, S](IMap.empty[J, S], Some((
        envT(
          TypeStat.coll(Real(4), Real(3).some, Real(4).some),
          TagST[J](Tagged(
            strings.StructuralString,
            envT(
              TypeStat.coll(Real(4), Real(3).some, Real(4).some),
              TypeST(TypeF.arr[J, S](IList(
                envT(
                  TypeStat.char(strSS("fbbq"), 'b', 'q'),
                  TypeST(TypeF.simple[J, S](SimpleType.Char))).embed,
                envT(
                  TypeStat.char(strSS("oaau"), 'a', 'u'),
                  TypeST(TypeF.simple[J, S](SimpleType.Char))).embed,
                envT(
                  TypeStat.char(strSS("orzu"), 'o', 'z'),
                  TypeST(TypeF.simple[J, S](SimpleType.Char))).embed,
                envT(
                  TypeStat.char(strSS("x"), 'x', 'x'),
                  TypeST(TypeF.const[J, S](J.char('x')))).embed
              ).left))).embed))).embed,
        envT(
          TypeStat.int(SampleStats.freq(Real(4), Real(1)), BigInt(1), BigInt(1)),
          TypeST(TypeF.const[J, S](J.int(1)))
        ).embed
      ))))).embed

    verify(config.copy(mapMaxSize = 3L, retainKeysSize = 0L, unionMaxSize = 1L), input, expected)
  }

  "coalesce map keys until <= max size, retaining top 2" >> {
    val input = List(
      _obj(ListMap("a" -> _int(1), "b" -> _int(1), "foo" -> _int(1))),
      _obj(ListMap("a" -> _int(1), "b" -> _int(1), "bar" -> _int(1))),
      _obj(ListMap("a" -> _int(1), "b" -> _int(1), "baz" -> _int(1))),
      _obj(ListMap("a" -> _int(1), "b" -> _int(1), "quux" -> _int(1)))
    )

    val fourOnes =
      envT(
        TypeStat.int(SampleStats.freq(Real(4), Real(1)), BigInt(1), BigInt(1)),
        TypeST(TypeF.const[J, S](J.int(1)))).embed

    val expected = envT(
      TypeStat.coll(Real(4), Real(3).some, Real(3).some),
      TypeST(TypeF.map[J, S](
        IMap(
          J.str("a") -> fourOnes,
          J.str("b") -> fourOnes),
        Some((envT(
          TypeStat.coll(Real(4), Real(3).some, Real(4).some),
          TagST[J](Tagged(
            strings.StructuralString,
            envT(
              TypeStat.coll(Real(4), Real(3).some, Real(4).some),
              TypeST(TypeF.arr[J, S](IList(
                envT(
                  TypeStat.char(strSS("fbbq"), 'b', 'q'),
                  TypeST(TypeF.simple[J, S](SimpleType.Char))).embed,
                envT(
                  TypeStat.char(strSS("oaau"), 'a', 'u'),
                  TypeST(TypeF.simple[J, S](SimpleType.Char))).embed,
                envT(
                  TypeStat.char(strSS("orzu"), 'o', 'z'),
                  TypeST(TypeF.simple[J, S](SimpleType.Char))).embed,
                envT(
                  TypeStat.char(strSS("x"), 'x', 'x'),
                  TypeST(TypeF.const[J, S](J.char('x')))).embed
              ).left))).embed))).embed,
          fourOnes))))).embed

    verify(config.copy(mapMaxSize = 3L, retainKeysSize = 2L, unionMaxSize = 1L), input, expected)
  }

  "coalesce new map keys when primary type in unknown key" >> {
    val input = List(
      _obj(ListMap("foo" -> _int(1))),
      _obj(ListMap("bar" -> _int(1))),
      _obj(ListMap("baz" -> _int(1))),
      _obj(ListMap("quux" -> _int(1)))
    )

    val expected = envT(
      TypeStat.coll(Real(4), Real(1).some, Real(1).some),
      TypeST(TypeF.map[J, S](IMap.empty[J, S], Some((
        envT(
          TypeStat.coll(Real(4), Real(3).some, Real(4).some),
          TagST[J](Tagged(
            strings.StructuralString,
            envT(
              TypeStat.coll(Real(4), Real(3).some, Real(4).some),
              TypeST(TypeF.arr[J, S](IList(
                envT(
                  TypeStat.char(strSS("fbbq"), 'b', 'q'),
                  TypeST(TypeF.simple[J, S](SimpleType.Char))).embed,
                envT(
                  TypeStat.char(strSS("oaau"), 'a', 'u'),
                  TypeST(TypeF.simple[J, S](SimpleType.Char))).embed,
                envT(
                  TypeStat.char(strSS("orzu"), 'o', 'z'),
                  TypeST(TypeF.simple[J, S](SimpleType.Char))).embed,
                envT(
                  TypeStat.char(strSS("x"), 'x', 'x'),
                  TypeST(TypeF.const[J, S](J.char('x')))).embed
              ).left))).embed))).embed,
        envT(
          TypeStat.int(SampleStats.freq(Real(4), Real(1)), BigInt(1), BigInt(1)),
          TypeST(TypeF.const[J, S](J.int(1)))
        ).embed
      ))))).embed

    verify(config.copy(mapMaxSize = 2L, retainKeysSize = 0L, unionMaxSize = 1L), input, expected)
  }

  "narrow unions until <= max size" >> {
    val input = List(
      _obj(ListMap("foo" -> _int(1))),
      _obj(ListMap("foo" -> _int(2))),
      _obj(ListMap("foo" -> _bool(true))),
      _obj(ListMap("foo" -> _bool(false)))
    )

    val expected = envT(
      TypeStat.coll(Real(4), Real(1).some, Real(1).some),
      TypeST(TypeF.map[J, S](IMap(
        J.str("foo") -> envT(
          TypeStat.count(Real(4)),
          TypeST(TypeF.coproduct[J, S](
            envT(
              TypeStat.int(
                SampleStats.fromFoldable(IList(Real(1), Real(2))),
                BigInt(1),
                BigInt(2)),
              TypeST(TypeF.simple[J, S](SimpleType.Int))).embed,
            envT(
              TypeStat.bool(Real(1), Real(1)),
              TypeST(TypeF.simple[J, S](SimpleType.Bool))).embed))
        ).embed
      ), None))).embed

    verify(config.copy(unionMaxSize = 2L), input, expected)
  }

  "coalesce consts with primary types in unions" >> {
    val input = List(
      _obj(ListMap("foo" -> _int(1))),
      _obj(ListMap("foo" -> _int(2))),
      _obj(ListMap("foo" -> _bool(true))),
      _obj(ListMap("foo" -> _int(3)))
    )

    val expected = envT(
      TypeStat.coll(Real(4), Real(1).some, Real(1).some),
      TypeST(TypeF.map[J, S](IMap(
        J.str("foo") -> envT(
          TypeStat.count(Real(4)),
          TypeST(TypeF.coproduct[J, S](
            envT(
              TypeStat.int(
                SampleStats.fromFoldable(IList(Real(1), Real(2), Real(3))),
                BigInt(1),
                BigInt(3)),
              TypeST(TypeF.simple[J, S](SimpleType.Int))).embed,
            envT(
              TypeStat.bool(Real(1), Real(0)),
              TypeST(TypeF.const[J, S](J.bool(true)))).embed))
        ).embed
      ), None))).embed

    verify(config.copy(unionMaxSize = 2L), input, expected)
  }
}

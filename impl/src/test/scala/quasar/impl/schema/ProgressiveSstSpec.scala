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

import slamdata.Predef.{Stream => _, _}
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

object ProgressiveSstSpec extends quasar.Qspec {
  import Data._, StructuralType.{TagST, TypeST}

  type J = Fix[EJson]
  type S = SST[J, Real]

  implicit val showReal: Show[Real] =
    Show.showFromToString

  val J = Fixed[J]
  val config = SstConfig[J, Real](1000L, 1000L, 0L, 0L, 1000L, true)

  def verify(cfg: SstConfig[J, Real], input: List[Data], expected: S) =
    Stream.emits(input map (SST.fromData[J, Real](Real.one, _)))
      .chunks
      .through(progressiveSst(cfg))
      .covary[cats.effect.IO]
      .compile.last.unsafeRunSync
      .must(beSome(equal(expected)))

  def strSS(s: String): SampleStats[Real] =
    SampleStats.fromFoldable(s.map(c => Real(c.toInt)).toList)

  "coalesce maps > max size" >> {
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
          TypeStat.str(Real(4), Real(3), Real(4), "bar", "quux"),
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
                  TypeST(TypeF.simple[J, S](SimpleType.Char))).embed
              ), None))).embed))).embed,
        envT(
          TypeStat.int(SampleStats.freq(Real(4), Real(1)), BigInt(1), BigInt(1)),
          TypeST(TypeF.const[J, S](J.int(1)))
        ).embed
      ))))).embed

    verify(config.copy(mapMaxSize = 3L, retainKeysSize = 0L), input, expected)
  }

  "coalesce maps > max size, retaining top 2" >> {
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
          TypeStat.str(Real(4), Real(3), Real(4), "bar", "quux"),
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
                  TypeST(TypeF.simple[J, S](SimpleType.Char))).embed
              ), None))).embed))).embed,
          fourOnes))))).embed

    verify(config.copy(mapMaxSize = 3L, retainKeysSize = 2L), input, expected)
  }
}

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

import slamdata.Predef.{List, Some, None}
import quasar.contrib.algebra._
import quasar.contrib.matryoshka.envT
import quasar.ejson.EJson
import quasar.ejson.implicits._
import quasar.fp._
import quasar.contrib.iota._
import quasar.tpe.{SimpleType, TypeF}
import StructuralType.{TagST, TypeST}

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import scalaz.{IList, NonEmptyList, Show}
import scalaz.syntax.std.option._
import scalaz.syntax.foldable1._
import spire.math.Real

final class StringsSpec extends quasar.Qspec {
  type J = Fix[EJson]

  implicit val realShow = Show.showFromToString[Real]

  "widen string yields an array of its characters" >> {
    val exp = EJson.arr(List('f', 'o', 'o') map (EJson.char[J](_)) : _*)
    val strS = TypeStat.fromEJson(Real(5), EJson.str[J]("foo"))
    val wid0 = SST.fromEJson(Real(5), exp)
    val wid = envT(strS, TagST[J](Tagged(strings.StructuralString, wid0))).embed
    strings.widen[J, Real](strS, "foo").embed must_= wid
  }

  "widenStats string yields a character array without literals" >> {
    val exp = IList('f', 'o', 'o') map (c => compression.primarySst(false)(Real(1), EJson.char[J](c)))
    val strS = TypeStat.fromEJson(Real(5), EJson.str[J]("foo"))
    val wid0 = envT(
      TypeStat.coll(Real(5), Real(3).some, Real(3).some),
      TypeST[J, SST[J, Real]](TypeF.arr(exp, None))).embed
    val wid = envT(strS, TagST[J](Tagged(strings.StructuralString, wid0))).embed
    strings.widenStats[J, Real](strS, "foo").embed must_= wid
  }

  "compress string yields a generic array sampled from its characters" >> {
    val char0 =
      NonEmptyList('f', 'o', 'o') foldMap1 (c => TypeStat.fromEJson(Real(1), EJson.char[J](c)))

    val char =
      envT(char0, TypeST(TypeF.simple[J, SST[J, Real]](SimpleType.Char))).embed

    val ss = TypeStat.str(Real(5), Real(3), Real(3), "foo", "foo")
    val cs = TypeStat.coll(Real(5), Some(Real(3)), Some(Real(3)))

    val comp =
      envT(ss, TagST[J](Tagged(
        strings.StructuralString,
        envT(cs, TypeST(TypeF.arr[J, SST[J, Real]](IList[SST[J, Real]](), Some(char)))).embed))).embed

    strings.compress[SST[J, Real], J, Real](ss, "foo").embed must_= comp
  }
}

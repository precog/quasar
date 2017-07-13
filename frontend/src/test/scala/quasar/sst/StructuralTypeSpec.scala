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
import quasar.contrib.matryoshka.arbitrary._
import quasar.{ejson => ejs}
import quasar.ejson.{CommonEJson => C, EJson, EJsonArbitrary, ExtEJson => E}
import quasar.ejson.implicits._
import quasar.fp._
import quasar.tpe._

import scala.Predef.$conforms

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import org.specs2.scalacheck._
import org.specs2.scalaz._
import scalaz._, Scalaz._
import scalaz.scalacheck.{ScalazProperties => propz}
import scalaz.scalacheck.ScalazArbitrary._

final class StructuralTypeSpec extends Spec
  with ScalazMatchers
  with StructuralTypeArbitrary
  with EJsonArbitrary
  with SimpleTypeArbitrary {

  implicit val params = Parameters(maxSize = 5)

  type J = Fix[EJson]
  type S = StructuralType[J, Int]

  def mkST(j: J): S = StructuralType.fromEJsonK[J](1, j)

  checkAll(propz.equal.laws[S])
  checkAll(propz.monoid.laws[S])
  checkAll(propz.traverse1.laws[StructuralType[J, ?]])
  // FIXME: Need Cogen
  //checkAll(propz.comonad.laws[StructuralType[J, ?]])

  "structural monoid" >> {
    "accumulates measure for identical structure" >> prop { (ejs: J, k0: Int) =>
      val k = scala.math.abs(k0 % 100) + 1
      val st = mkST(ejs)
      st.multiply(k) must equal(st as k)
    }

    "unmergeable values accumulate via union" >> prop { (x: J, y: J) =>
      ((compositeTypeOf(x) ≠ compositeTypeOf(y)) && (x ≠ y)) ==> {
      val (xst, yst) = (mkST(x), mkST(y))
      val exp = envT(2, TypeF.coproduct[J, S](xst, yst)).embed
      (xst |+| yst) must equal(exp)
    }}

    "known and unknown arrays merge union of known with lub" >> prop { (xs: List[J], y: SimpleType) =>
      val arr = mkST(C(ejs.Arr(xs)).embed)
      val ySimple = envT(1, TypeF.simple[J, S](y)).embed
      val lubArr = envT(1, TypeF.arr[J, S](ySimple.right)).embed
      val consts = xs.toIList.map(mkST)

      val exp = envT(2, TypeF.arr[J, S](\/-(consts match {
        case INil()                 => ySimple
        case ICons(a, INil())       => envT(2, TypeF.coproduct[J, S](ySimple, a)).embed
        case ICons(a, ICons(b, cs)) => envT(2, TypeF.union[J, S](ySimple, a, b :: cs)).embed
      }))).embed

      (lubArr |+| arr) must equal(exp)
    }

    "merging known map with unknown results in a map with both" >> prop { (kn: IMap[J, S], unk: (S, S)) =>
      val m1 = envT(1, TypeF.map[J, S](kn, None)).embed
      val m2 = envT(1, TypeF.map[J, S](IMap.empty[J, S], Some(unk))).embed
      val me = envT(2, TypeF.map[J, S](kn, Some(unk))).embed
      (m1 |+| m2) must equal(me)
    }

    "merging known maps where some keys differ results in combination of keys" >> prop { m0: IMap[J, S] =>
      m0.minViewWithKey map { case ((k, v), m) =>
        val m1 = envT(1, TypeF.map[J, S](IMap.singleton(k, v), None)).embed
        val m2 = envT(1, TypeF.map[J, S](m, None)).embed
        val m3 = envT(2, TypeF.map[J, S](m0, None)).embed
        (m1 |+| m2) must equal(m3)
      } getOrElse ok
    }

    "unions merge by accumulating mergeable values" >> prop { (xs: IList[S], ys: IList[S], i: BigInt, s: String, st: SimpleType) =>
      val xst = envT(1, TypeF.arr[J, S](xs.left)).embed
      val yst = envT(1, TypeF.arr[J, S](ys.left)).embed
      val ist = mkST(E(ejs.int[J](i)).embed)
      val sst = mkST(C(ejs.str[J](s)).embed)
      val pst = envT(1, TypeF.simple[J, S](st)).embed
      val a   = envT(1, TypeF.union[J, S](xst, ist, IList(pst))).embed
      val b   = envT(1, TypeF.union[J, S](yst, sst, IList(pst))).embed
      val c   = envT(2, TypeF.union[J, S](xst |+| yst, ist, IList(sst, pst |+| pst))).embed
      (a |+| b) must equal(c)
    }
  }
}

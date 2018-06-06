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

import slamdata.Predef._
import quasar.contrib.matryoshka._
import quasar.contrib.matryoshka.arbitrary._
import quasar.contrib.specs2.Spec
import quasar.{ejson => ejs}
import quasar.ejson.{Decoded, DecodeEJson, EJson, EJsonArbitrary, EncodeEJson}
import quasar.ejson.implicits._
import quasar.fp._, Helpers._
import quasar.contrib.iota._
import quasar.tpe._

import scala.Predef.$conforms

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import monocle.syntax.fields._
import org.specs2.scalacheck._
import scalaz._, Scalaz._
import scalaz.scalacheck.{ScalazProperties => propz}
import scalaz.scalacheck.ScalazArbitrary._

final class StructuralTypeSpec extends Spec
  with StructuralTypeArbitrary
  with EJsonArbitrary
  with SimpleTypeArbitrary {

  import StructuralType.{TagST, TypeST}

  implicit val params = Parameters(maxSize = 5)

  type J = TypedEJson[Fix]
  type S = StructuralType[J, Int]

  val J = ejs.Fixed[J]
  val metaTag = J.meta composeLens _2 composePrism J.tpe

  def mkST(j: J): S = StructuralType.fromEJsonK[J](1, j)

  def diffTags(x: J, y: J): Boolean =
    (metaTag.getOption(x) |@| metaTag.getOption(y))(_ ≠ _) | true

  checkAll(propz.equal.laws[S])
  checkAll(propz.semigroup.laws[S])
  checkAll(propz.traverse1.laws[StructuralType[J, ?]])
  // FIXME: Need Cogen
  //checkAll(propz.comonad.laws[StructuralType[J, ?]])

  "structural semigroup" >> {
    "accumulates measure for identical structure" >> prop { (ejs: J, k0: Int) =>
      val k = scala.math.abs(k0 % 100) + 1
      val st = mkST(ejs)
      Semigroup[S].multiply1(st, k) must equal(st as (k + 1))
    }

    "unmergeable values accumulate via union" >> prop { (x: J, y: J) =>
      ((compositeTypeOf(x) ≠ compositeTypeOf(y)) && diffTags(x, y) && (x ≠ y)) ==> {
      val (xst, yst) = (mkST(x), mkST(y))
      val exp = envT(2, TypeST(TypeF.coproduct[J, S](xst, yst))).embed
      (xst |+| yst) must equal(exp)
    }}

    "known and unknown arrays merge union of known with lub" >> prop { (xs: List[J], y: SimpleType) =>
      val arr = mkST(J.arr(xs))
      val ySimple = envT(1, TypeST(TypeF.simple[J, S](y))).embed
      val lubArr = envT(1, TypeST(TypeF.arr[J, S](ySimple.right))).embed
      val consts = xs.toIList.map(mkST)

      val exp = envT(2, TypeST(TypeF.arr[J, S](\/-(consts match {
        case INil() => ySimple

        case ICons(a, INil()) =>
          envT(2, TypeST(TypeF.coproduct[J, S](ySimple, a))).embed

        case ICons(a, ICons(b, cs)) =>
          StructuralType.disjoinUnionsƒ[J, Int, StructuralType.ST[J, ?], S].apply(
            envT(2, TypeST(TypeF.union[J, S](ySimple, a, b :: cs)))
          ).embed
      })))).embed

      (lubArr |+| arr) must equal(exp)
    }

    "merging known map with unknown results in a map with both" >> prop { (kn: IMap[J, S], unk: (S, S)) =>
      val m1 = envT(1, TypeST(TypeF.map[J, S](kn, None))).embed
      val m2 = envT(1, TypeST(TypeF.map[J, S](IMap.empty[J, S], Some(unk)))).embed
      val me = envT(2, TypeST(TypeF.map[J, S](kn, Some(unk)))).embed
      (m1 |+| m2) must equal(me)
    }

    "merging known maps where some keys differ results in combination of keys" >> prop { m0: IMap[J, S] =>
      m0.minViewWithKey map { case ((k, v), m) =>
        val m1 = envT(1, TypeST(TypeF.map[J, S](IMap.singleton(k, v), None))).embed
        val m2 = envT(1, TypeST(TypeF.map[J, S](m, None))).embed
        val m3 = envT(2, TypeST(TypeF.map[J, S](m0, None))).embed
        (m1 |+| m2) must equal(m3)
      } getOrElse ok
    }

    "unions merge by accumulating mergeable values" >> prop { (xs: IList[S], ys: IList[S], i: BigInt, s: String, st: SimpleType) =>
      val xst = envT(1, TypeST(TypeF.arr[J, S](xs.left))).embed
      val yst = envT(1, TypeST(TypeF.arr[J, S](ys.left))).embed
      val ist = mkST(J.int(i))
      val sst = mkST(J.str(s))
      val pst = envT(1, TypeST(TypeF.simple[J, S](st))).embed
      val a   = envT(1, TypeST(TypeF.union[J, S](xst, ist, IList(pst)))).embed
      val b   = envT(1, TypeST(TypeF.union[J, S](yst, sst, IList(pst)))).embed
      val c   = envT(2, TypeST(TypeF.union[J, S](xst |+| yst, ist, IList(sst, pst |+| pst)))).embed
      (a |+| b) must equal(c)
    }

    "same type tag merges contents" >> prop { (t: ejs.TypeTag, s1: S, s2: S) =>
      val x = envT(1, TagST[J](Tagged(t, s1))).embed
      val y = envT(1, TagST[J](Tagged(t, s2))).embed
      val z = envT(2, TagST[J](Tagged(t, s1 |+| s2))).embed
      (x |+| y) must equal(z)
    }

    "different tags do not merge" >> prop { (t1: ejs.TypeTag, s: S) =>
      val t2 = ejs.TypeTag(t1.value + "_2")
      val x = envT(1, TagST[J](Tagged(t1, s))).embed
      val y = envT(1, TagST[J](Tagged(t2, s))).embed
      val z = envT(2, TypeST(TypeF.coproduct[J, S](x, y))).embed
      (x |+| y) must equal(z)
    }
  }

  "EJson codec" >> prop { st: StructuralType[Fix[EJson], Int] =>
    DecodeEJson[StructuralType[Fix[EJson], Int]].decode(
      EncodeEJson[StructuralType[Fix[EJson], Int]].encode[Fix[EJson]](
        st)) ≟ st.point[Decoded]
  }
}

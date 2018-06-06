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

package quasar.tpe

import slamdata.Predef._
import quasar.contrib.algebra._
import quasar.contrib.matryoshka._
import quasar.contrib.matryoshka.arbitrary._
import quasar.contrib.specs2.Spec
import quasar.ejson.{Decoded, DecodeEJson, EncodeEJson, EJson, EJsonArbitrary, Fixed}
import quasar.ejson.implicits._
import quasar.fp._, Helpers._
import quasar.contrib.iota._

import scala.Predef.$conforms

import algebra.laws._
import matryoshka.data.Fix
import matryoshka.implicits._
import org.specs2.scalacheck._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazProperties.{equal => eql, _}
import scalaz.scalacheck.ScalazArbitrary._

final class TypeFSpec extends Spec with TypeFArbitrary with EJsonArbitrary {
  // NB: Limit type depth to something reasonable.
  implicit val params = Parameters(maxSize = 10)

  type J = Fix[EJson]
  type T = Fix[TypeF[J, ?]]

  val J = Fixed[J]

  implicit def typeFIntEqual[A: Equal]: Equal[TypeF[Int, A]] =
    TypeF.structuralEqual(Equal[Int])(Equal[A])

  checkAll("structural", eql.laws[TypeF[Int, String]])
  checkAll(traverse.laws[TypeF[Int, ?]])
  // TODO: Want to check cats.kernel.OrderLaws, but need Cogen
  checkAll("subtyping", eql.laws[T])
  checkAll(LatticeLaws[T].boundedDistributiveLattice.all)
  checkAll(LatticePartialOrderLaws[T].boundedLatticePartialOrder.all)

  "identical" >> {
    "unions that differ only in order are identical" >> prop { (x: T, y: T, z: T, zs: IList[T]) =>
      TypeF.identical[J](
        TypeF.union[J, T](x, y, z :: zs).embed,
        TypeF.union[J, T](y, z, x :: zs).embed
      ) must beTrue
    }
  }

  "subtyping" >> {
    import TypeF.{map => tmap, _}

    def orT(t: T): T =
      isBottom[J](normalization.normalize[J](t)).fold(top[J, T]().embed, t)

    "t ≟ (t|t)" >> prop { t: T =>
      t ≟ coproduct[J, T](t, t).embed
    }

    "t <: (t|u)" >> prop { (t: T, u: T) =>
      isSubtypeOf[J](t, coproduct[J, T](t, u).embed)
    }

    "<ejs> <: SimpleType" >> prop { j: J =>
      simpleTypeOf(j) all { st =>
        isSubtypeOf[J](const[J, T](j).embed, simple[J, T](st).embed)
      }
    }

    "<map> <: {}" >> prop { (kn: IMap[J, T], unk: Option[(T, T)]) =>
      isSubtypeOf[J](
        tmap[J, T](kn              ,  unk).embed,
        tmap[J, T](IMap.empty[J, T], None).embed)
    }

    "{m ? t : u} <: {m}" >> prop { (kn: IMap[J, T], uk: T, uv: T) =>
      isSubtypeOf[J](
        tmap[J, T](kn, Some((uk, uv))).embed,
        tmap[J, T](kn,           None).embed)
    }

    "{m} ∪ {k:v} <: {m}" >> prop { kn: IMap[J, T] =>
      val normM =
        kn.mapKeys(_.transCata[J](EJson.elideMetadata[J]))

      normM.maxViewWithKey forall { case ((k, v), m) =>
        isSubtypeOf[J](
          tmap[J, T](m + (k -> v), None).embed,
          tmap[J, T](m,            None).embed)
      }
    }

    "{? t : u} <: {? v : w} iff (t <: v) && (u <: w)" >> prop { (a: T, b: T, c: T, d: T) =>
      val (t, u, v, w) = (orT(a), orT(b), orT(c), orT(d))
      val m = tmap[J, T](IMap.empty[J, T], Some((t, u))).embed
      val n = tmap[J, T](IMap.empty[J, T], Some((v, w))).embed
      isSubtypeOf[J](m, n) ≟ (isSubtypeOf[J](t, v) && isSubtypeOf[J](u, w))
    }

    "<array> <: []" >> prop { a: IList[T] \/ T =>
      isSubtypeOf[J](
        arr[J, T](a           ).embed,
        arr[J, T](IList().left).embed)
    }

    "[x, y] <: z[] iff (x <: z) && (y <: z)" >> prop { (a: T, b: T, c: T) =>
      val (x, y, z) = (orT(a), orT(b), orT(c))
      isSubtypeOf[J](
        arr[J, T](IList(x, y).left).embed,
        arr[J, T](         z.right).embed
      ) ≟ (isSubtypeOf[J](x, z) && isSubtypeOf[J](y, z))
    }

    "[t, u] <: [v, w] iff (t <: v) && (u <: w)" >> prop { (a: T, b: T, c: T, d: T) =>
      val (t, u, v, w) = (orT(a), orT(b), orT(c), orT(d))
      isSubtypeOf[J](
        arr[J, T](IList(t, u).left).embed,
        arr[J, T](IList(v, w).left).embed
      ) ≟ (isSubtypeOf[J](t, v) && isSubtypeOf[J](u, w))
    }

    "[a, b] <: [a]" >> prop { (as: IList[T], b: T) =>
      isSubtypeOf[J](
        arr[J, T]((as ::: IList(b)).left).embed,
        arr[J, T]( as.left              ).embed)
    }

    "(x ∧ y) <: x && (x ∧ y) <: y" >> prop { (x: T, y: T) =>
      val z = glb[J](x, y)
      (isSubtypeOf[J](z, x) && isSubtypeOf[J](z, y))
    }

    "x <: (x ∨ y) && y <: (x ∨ y)" >> prop { (x: T, y: T) =>
      val z = lub[J](x, y)
      (isSubtypeOf[J](x, z) && isSubtypeOf[J](y, z))
    }
  }

  "EJson codec" >> prop { t: T =>
    DecodeEJson[T].decode(EncodeEJson[T].encode[J](t)) ≟ t.point[Decoded]
  }
}

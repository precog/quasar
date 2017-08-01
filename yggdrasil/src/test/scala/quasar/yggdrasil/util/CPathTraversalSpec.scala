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

package quasar.yggdrasil

import quasar.precog.common._
import quasar.precog._, TestSupport._
import quasar.precog.util._
import quasar.yggdrasil.table._

import java.time.ZonedDateTime

import scala.specialized

class CPathTraversalSpec extends Specification {
  import CPathTraversal._

  sealed trait ColBuilder[@specialized(Boolean, Long, Double) A] {
    def apply(defined: BitSet, items: Array[A]): Column
  }

  object ColBuilder {
    private def builder[@specialized(Boolean, Long, Double) A](f: (BitSet, Array[A]) => Column): ColBuilder[A] = {
      new ColBuilder[A] {
        def apply(defined: BitSet, items: Array[A]): Column = f(defined, items)
      }
    }

    implicit val LongColBuilder   = builder[Long](ArrayLongColumn(_, _))
    implicit val StrColBuilder    = builder[String](ArrayStrColumn(_, _))
    implicit val BoolColBuilder   = builder[Boolean](ArrayBoolColumn(_, _))
    implicit val DoubleColBuilder = builder[Double](ArrayDoubleColumn(_, _))
    implicit val NumColBuilder    = builder[BigDecimal](ArrayNumColumn(_, _))
    implicit val DateColBuilder   = builder[ZonedDateTime](ArrayDateColumn(_, _))
    implicit def HomogeneousArrayColBuilder[@specialized(Boolean, Long, Double) A: CValueType] =
      builder[Array[A]](ArrayHomogeneousArrayColumn(_, _))
  }

  "constructing CPathTraversal from CPaths" should {
    "handle trivial select" in {
      val t = CPathTraversal(List(CPath("a[*].b[0]")))
      t must_== Select(CPathField("a"), Select(CPathArray, Select(CPathField("b"), Select(CPathIndex(0), Done))))
    }

    "handle CPath with simple array intersection" in {
      val t = CPathTraversal(List(CPath("[*][0]"), CPath("[*][1]")))
      t must_== Select(CPathArray, Sequence(List(
        Select(CPathIndex(0), Done),
        Select(CPathIndex(1), Done)
      )))
    }
  }
  def col[@specialized(Boolean, Long, Double) A](defined: Int*)(values: A*)(implicit builder: ColBuilder[A], m: CTag[A]) = {
    val max = defined.max + 1
    val column = m.newArray(max)
    (defined zip values) foreach { case (i, x) =>
      column(i) = x
    }
    builder(BitSetUtil.create(defined), column)
  }

  "rowOrder" should {
    "order columns without arrays" in {

      // This is mostly to ensure that CPathTraversal orderings only use the
      // paths original given to it.

      // { a: 2, b: [ 0, "a" ] }
      // { a: 1, b: [ 0, "b" ] }

      val cols = Map(
        CPath("a") -> Set(col(0, 1)(2L, 1L)),
        CPath("b[0]") -> Set(col(0, 1)(0L, 0L)),
        CPath("b[1]") -> Set(col(0, 1)("a", "b"))
      )
      val allPaths = cols.keys.toList
      val order = CPathTraversal(allPaths).rowOrder(allPaths, cols)
      order.eqv(0, 0) must beTrue
      order.gt(0, 1) must beTrue
      order.lt(0, 1) must beFalse

      val subPaths = List(CPath("b[0]"), CPath("b[1]"))
      val order2 = CPathTraversal(subPaths).rowOrder(allPaths, cols)
      order2.lt(0, 1) must beTrue
      order2.gt(0, 1) must beFalse
      order2.eqv(1, 1)

      val singlePath = List(CPath("b[0]"))
      val order3 = CPathTraversal(singlePath).rowOrder(allPaths, cols)
      order3.eqv(0, 1) must beTrue
    }

    "order non-intersecting arrays by length" in {
      val cols: Map[CPath, Set[Column]] = Map(
        CPath("[*]") -> Set(ArrayHomogeneousArrayColumn(Array(
          Array(0L, 1L), Array(0L), Array(0L, 1L, 2L)
        )))
      )
      val paths = cols.keys.toList

      val t = CPathTraversal(paths)
      val order = t.rowOrder(paths, cols)
      order.eqv(0, 0) must beTrue
      order.eqv(1, 1) must beTrue
      order.eqv(0, 2) must beFalse
      order.eqv(0, 1) must beFalse
      order.eqv(1, 0) must beFalse

      order.lt(0, 2) must beTrue
      order.lt(2, 0) must beFalse
      order.gt(0, 1) must beTrue
    }

    // false
    // { foo: 5 }

    // . = undefined, .foo = 5
    // . = false, .foo = undefined
    val valueAndObject: Map[CPath, Set[Column]] = Map(
      CPath.Identity -> Set(col(0)(false)),
      CPath("foo") -> Set(col(1)(5L))
    )

    "order value and object" in {
      val paths = valueAndObject.keys.toList
      val t = CPathTraversal(paths)
      val order = t.rowOrder(paths, valueAndObject)
      order.gt(0, 1) must beTrue
    }

    // [0, 1, 2]
    // [0, "b"]
    // ["abc", "c"]
    val nonIntersectingHet: Map[CPath, Set[Column]] = Map(
      CPath("[*]") -> Set(col(0)(Array[Long](0L, 1L, 2L))),
      CPath("[0]") -> Set(col(1)(0L), col(2)("abc")),
      CPath("[1]") -> Set(col(1, 2)("b", "c"))
    )

    "order non-intersecting heterogeneous arrays" in {

      val paths = nonIntersectingHet.keys.toList
      val t = CPathTraversal(paths)
      val order = t.rowOrder(paths, nonIntersectingHet)
      order.gt(0, 1) must beTrue
      order.gt(1, 2) must beTrue
      order.gt(0, 2) must beTrue
      order.gt(1, 0) must beFalse
      order.gt(2, 1) must beFalse
      order.gt(2, 0) must beFalse
      order.eqv(1, 1)
      order.eqv(2, 2)
    }

    "order part of non-intersecting heterogeneous arrays" in {

      // Uses only the first element in the arrays to order. This means, the
      // 1st and 2nd elements are equal, since they both have 0L as the first
      // element.

      val paths = nonIntersectingHet.keys.toList
      val t = CPathTraversal(List(CPath("[0]")))
      val order = t.rowOrder(paths, nonIntersectingHet)
      order.eqv(0, 1)
      order.lt(2, 1)
      order.lt(2, 0)
    }

    val intersectingHom: Map[CPath, Set[Column]] = Map(
      CPath("[*][0]") -> Set(ArrayHomogeneousArrayColumn(Array(
        Array(0L, 1L, 2L),
        Array(0L, 1L, 1L),
        Array(0L, 1L, 2L)))),
      CPath("[*][1]") -> Set(ArrayHomogeneousArrayColumn(Array(
        Array("a", "b", "c"),
        Array("a", "c", "c"),
        Array("a", "b", "c"))))
    )

    "order simple intersecting arrays" in {
      // [0, "a"], [1, "b"], [2, "c"]
      // [0, "a"], [1, "c"], [1, "c"]
      val cols = intersectingHom
      val paths = cols.keys.toList

      val t = CPathTraversal(paths)
      val order = t.rowOrder(paths, cols)
      order.lt(0, 1) must beTrue
      order.lt(1, 0) must beFalse
      order.eqv(0, 0) must beTrue
      order.eqv(1, 1) must beTrue
      order.eqv(0, 1) must beFalse
      order.eqv(1, 0) must beFalse

      order.eqv(0, 2) must beTrue
      order.lt(0, 2) must beFalse
      order.gt(1, 2) must beTrue
    }

    "order objets in arrays" in {
      // { "kids": [ { "name": "Tom", "age": 27 }, { "name": "Eric", "age": 29 } ] }
      // { "kids": [ { "name": "Emily", "age": 21 }, { "name": "Amanda", "age": 27 } ] }
      val cols = Map(
        CPath("kids[*].name") -> Set(col(0, 1)(Array("Tom", "Eric"), Array("Amanda", "Emily"))),
        CPath("kids[*].age") -> Set(col(0, 1)(Array(27L, 29L), Array(27L, 21L)))
      )

      val paths = cols.keys.toList
      val all = CPathTraversal(paths)
      val ages = CPathTraversal(CPath("kids[*].age"))
      val name0 = CPathTraversal(CPath("kids[0].name"))
      val name1 = CPathTraversal(CPath("kids[1].name"))

      val order = all.rowOrder(paths, cols)
      val agesOrder = ages.rowOrder(paths, cols)
      val name0Order = name0.rowOrder(paths, cols)
      val name1Order = name1.rowOrder(paths, cols)

      order.gt(0, 1) must beTrue
      name0Order.gt(0, 1) must beTrue
      name1Order.lt(1, 0) must beTrue
      agesOrder.lt(1, 0) must beTrue
    }

    "order parts of intersecting arrays" in {
      val cols = intersectingHom
      val t = CPathTraversal(List("[0][0]", "[0][1]", "[1][0]", "[1][1]") map (CPath(_)))
      val order = t.rowOrder(cols.keys.toList, cols)
      order.eqv(0, 2) must beTrue
      order.lt(0, 1) must beTrue
      order.lt(2, 1) must beTrue
    }
  }
}



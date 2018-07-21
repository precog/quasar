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

package quasar.qsu

import slamdata.Predef._
import quasar.{Qspec, TreeMatchers}
import quasar.common.{SortDir, JoinType}
import quasar.contrib.matryoshka._
import quasar.contrib.pathy.AFile
import quasar.ejson.implicits._
import quasar.ejson.{EJson, Fixed}
import quasar.fp._
import quasar.contrib.iota._
import quasar.qscript.construction
import quasar.qscript.{MapFuncsCore, JoinSide, LeftSide, PlannerError, RightSide}
import quasar.{Qspec, TreeMatchers}

import matryoshka._
import matryoshka.data._
import pathy.Path
import scalaz.{\/, \/-, EitherT, ICons, INil, Need, NonEmptyList => NEL, StateT, Free}
import scalaz.Scalaz._

object ExtractFreeMapSpec extends Qspec with QSUTTypes[Fix] with TreeMatchers {
  import QScriptUniform.{DTrans, Retain, Rotation}
  import QSUGraph.Extractors._

  type F[A] = EitherT[StateT[Need, Long, ?], PlannerError, A]
  type QSU[A] = QScriptUniform[A]

  val ejs = Fixed[Fix[EJson]]
  val qsu = QScriptUniform.DslT[Fix]
  val func = construction.Func[Fix]
  val recFunc = construction.RecFunc[Fix]

  def projectStrKey(key: String): FreeMap = func.ProjectKeyS(func.Hole, key)
  def recProjectStrKey(key: String): RecFreeMap = recFunc.ProjectKeyS(recFunc.Hole, key)

  def makeMap(left: String, right: String): JoinFunc =
    func.StaticMapS(
      left -> func.LeftSide,
      right -> func.RightSide)

  val orders: AFile = Path.rootDir </> Path.dir("client") </> Path.file("orders")
  val customers: AFile = Path.rootDir </> Path.dir("client") </> Path.file("customers")

  def extractFM(graph: QSUGraph) = ExtractFreeMap[Fix, F](graph)

  "extracting mappable region" should {

    "convert mappable filter predicate" >> {
      val predicate = recProjectStrKey("foo")

      val graph = QSUGraph.fromTree[Fix](
        qsu.lpFilter(
          qsu.read(orders),
          qsu.map(qsu.read(orders), predicate)))

      evaluate(extractFM(graph)) must beLike {
        case \/-(QSFilter(Read(`orders`), fm)) => fm must_= predicate
      }
    }

    "convert transposed filter predicate" >> {
      val predicate = recProjectStrKey("foo")

      val graph = QSUGraph.fromTree[Fix](
        qsu.lpFilter(
          qsu.read(orders),
          qsu.transpose(qsu.map(qsu.read(orders), predicate), Retain.Values, Rotation.ShiftMap)))

      evaluate(extractFM(graph)) must beLike {
        case \/-(Map(
            QSFilter(
              AutoJoin2(
                Read(`orders`),
                Transpose(Map(Read(`orders`), fm), Retain.Values, Rotation.ShiftMap),
                autojoinCondition),
              filterPredicate),
            valueAccess)) =>

          fm.linearize must beTreeEqual(predicate.linearize)

          filterPredicate.linearize must beTreeEqual(projectStrKey("filter_predicate_0"))

          valueAccess.linearize must beTreeEqual(projectStrKey("filter_source"))

          autojoinCondition must beTreeEqual(makeMap("filter_source", "filter_predicate_0"))
      }
    }

    "greedily convert mappable function applied to transposed filter predicate" >> {
      val field = recProjectStrKey("foo")

      val predicate =
        func.Gt(func.Hole, func.Constant(ejs.int(42)))

      val graph = QSUGraph.fromTree[Fix](
        qsu.lpFilter(
          qsu.read(orders),
          qsu.autojoin2((
            qsu.transpose(qsu.map(qsu.read(orders), field), Retain.Values, Rotation.ShiftMap),
            qsu.cint(42),
            _(MapFuncsCore.Gt(_, _))))))

      evaluate(extractFM(graph)) must beLike {
        case \/-(Map(
            QSFilter(
              AutoJoin2(
                Read(`orders`),
                Transpose(Map(Read(`orders`), fm), Retain.Values, Rotation.ShiftMap),
                autojoinCondition),
              filterPredicate),
            valueAccess)) =>

          fm.linearize must beTreeEqual(field.linearize)

          filterPredicate.linearize must beTreeEqual(predicate >> projectStrKey("filter_predicate_0"))

          valueAccess.linearize must beTreeEqual(projectStrKey("filter_source"))

          autojoinCondition must beTreeEqual(makeMap("filter_source", "filter_predicate_0"))
      }
    }

    "convert mappable group key" >> {
      val key = recProjectStrKey("foo")

      val graph = QSUGraph.fromTree[Fix](
        qsu.groupBy(
          qsu.read(orders),
          qsu.map(qsu.read(orders), key)))

      evaluate(extractFM(graph)) must beLike {
        case \/-(DimEdit(Read(`orders`), DTrans.Group(fm))) => fm must_= key.linearize
      }
    }

    "convert transposed group key" >> {
      val key = recProjectStrKey("foo")

      val graph = QSUGraph.fromTree[Fix](
        qsu.groupBy(
          qsu.read(orders),
          qsu.transpose(qsu.map(qsu.read(orders), key), Retain.Values, Rotation.ShiftMap)))

      evaluate(extractFM(graph)) must beLike {
        case \/-(Map(
            DimEdit(
              AutoJoin2(
                Read(`orders`),
                Transpose(Map(Read(`orders`), fm), Retain.Values, Rotation.ShiftMap),
                autojoinCondition),
              DTrans.Group(groupKey)),
            valueAccess)) =>

          fm.linearize must beTreeEqual(key.linearize)

          groupKey must beTreeEqual(projectStrKey("group_key_0"))

          valueAccess.linearize must beTreeEqual(projectStrKey("group_source"))

          autojoinCondition must beTreeEqual(makeMap("group_source", "group_key_0"))
      }
    }

    "greedily convert mappable function applied to transposed group key" >> {
      val key = recProjectStrKey("foo")

      val f =
        recFunc.Add(recFunc.Modulo(recFunc.Hole, recFunc.Constant(ejs.int(13))), recFunc.Constant(ejs.int(1)))

      val graph = QSUGraph.fromTree[Fix](
        qsu.groupBy(
          qsu.read(orders),
          qsu.map(
            qsu.transpose(qsu.map(qsu.read(orders), key), Retain.Values, Rotation.ShiftMap),
            f)))

      evaluate(extractFM(graph)) must beLike {
        case \/-(Map(
            DimEdit(
              AutoJoin2(
                Read(`orders`),
                Transpose(Map(Read(`orders`), fm), Retain.Values, Rotation.ShiftMap),
                autojoinCondition),
              DTrans.Group(groupKey)),
            valueAccess)) =>

          fm.linearize must beTreeEqual(key.linearize)

          groupKey must beTreeEqual(f.linearize >> projectStrKey("group_key_0"))

          valueAccess.linearize must beTreeEqual(projectStrKey("group_source"))

          autojoinCondition must beTreeEqual(makeMap("group_source", "group_key_0"))
      }
    }

    "convert mappable sort keys" >> {
      val key1 = recProjectStrKey("foo")
      val key2 = recProjectStrKey("foo")

      val graph: QSUGraph = QSUGraph.fromTree[Fix](
        qsu.lpSort(
          qsu.read(orders),
          NEL[(Fix[QSU], SortDir)](
            qsu.map(qsu.read(orders), key1) -> SortDir.Ascending,
            qsu.map(qsu.read(orders), key2) -> SortDir.Descending)))

      evaluate(extractFM(graph)) must beLike {
        case \/-(QSSort(
            Read(`orders`),
            Nil,
            NEL((fm1, SortDir.Ascending), ICons((fm2, SortDir.Descending), INil())))) =>
          fm1 must_= key1.linearize
          fm2 must_= key2.linearize
      }
    }

    "convert transposed sort key and mappable sort key" >> {
      val key1 = recProjectStrKey("foo")
      val key2 = recProjectStrKey("bar")

      val graph: QSUGraph = QSUGraph.fromTree[Fix](
        qsu.lpSort(
          qsu.read(orders),
          NEL[(Fix[QSU], SortDir)](
            qsu.map(qsu.read(orders), key1) -> SortDir.Ascending,
            qsu.transpose(qsu.map(qsu.read(orders), key2), Retain.Values, Rotation.ShiftMap) -> SortDir.Descending)))

      evaluate(extractFM(graph)) must beLike {
        case \/-(Map(
            QSSort(
              AutoJoin2(
                Read(`orders`),
                Transpose(Map(Read(`orders`), fm), Retain.Values, Rotation.ShiftMap),
                autojoinCondition),
              Nil,
              NEL((fm1, SortDir.Ascending), ICons((fm2, SortDir.Descending), INil()))),
            valueAccess)) => {

          fm.linearize must beTreeEqual(key2.linearize)

          fm1 must beTreeEqual(key1.linearize >> projectStrKey("sort_source"))

          fm2 must beTreeEqual(projectStrKey("sort_key_0"))

          valueAccess.linearize must beTreeEqual(projectStrKey("sort_source"))

          autojoinCondition must beTreeEqual(makeMap("sort_source", "sort_key_0"))
        }
      }
    }

    "convert two transposed sort keys and two mappable sort keys" >> {
      val key1 = recProjectStrKey("key1")
      val key2 = recProjectStrKey("key2")
      val key3 = recProjectStrKey("key3")
      val key4 = recProjectStrKey("key4")

      val graph: QSUGraph = QSUGraph.fromTree[Fix](
        qsu.lpSort(
          qsu.read(orders),
          NEL[(Fix[QSU], SortDir)](
            qsu.map(qsu.read(orders), key1) -> SortDir.Ascending,
            qsu.transpose(qsu.map(qsu.read(orders), key2), Retain.Values, Rotation.ShiftMap) -> SortDir.Descending,
            qsu.map(qsu.read(orders), key3) -> SortDir.Descending,
            qsu.transpose(qsu.map(qsu.read(orders), key4), Retain.Identities, Rotation.ShiftArray) -> SortDir.Ascending)))

      evaluate(extractFM(graph)) must beLike {
        case \/-(Map(
            QSSort(
              AutoJoin2(
                AutoJoin2(
                  Read(`orders`),
                  Transpose(Map(Read(`orders`), innerFM), Retain.Values, Rotation.ShiftMap),
                  innerAutojoinCondition),
                Transpose(Map(Read(`orders`), outerFM), Retain.Identities, Rotation.ShiftArray),
                outerAutojoinCondition),
              Nil,
              NEL(
                (fm1, SortDir.Ascending), ICons((fm2, SortDir.Descending), ICons((fm3, SortDir.Descending), ICons((fm4, SortDir.Ascending), INil()))))),
            valueAccess)) => {

          innerFM.linearize must beTreeEqual(key2.linearize)

          outerFM.linearize must beTreeEqual(key4.linearize)

          fm1 must beTreeEqual(key1.linearize >> projectStrKey("sort_source"))

          fm2 must beTreeEqual(projectStrKey("sort_key_0"))

          fm3 must beTreeEqual(key3.linearize >> projectStrKey("sort_source"))

          fm4 must beTreeEqual(projectStrKey("sort_key_1"))

          valueAccess.linearize must beTreeEqual(projectStrKey("sort_source"))

          innerAutojoinCondition must beTreeEqual(makeMap("sort_source", "sort_key_0"))

          outerAutojoinCondition must beTreeEqual(func.ConcatMaps(
            func.LeftSide,
            func.MakeMap(func.Constant(ejs.str("sort_key_1")), func.RightSide)))
        }
      }
    }

    "convert a partially mappable join condition on both sides" >> {
      val projectCustomersKey = recProjectStrKey("customers_key")
      val projectCustomersKey2 = recProjectStrKey("customers_key_2")
      val projectFoo = recProjectStrKey("foo")

      val graph = QSUGraph.fromTree[Fix](
        qsu.lpJoin(
          qsu.transpose(qsu.read(orders), Retain.Values, Rotation.ShiftMap),
          qsu.transpose(qsu.read(customers), Retain.Values, Rotation.ShiftMap),
          qsu._autojoin2(
            qsu.map(qsu.union(qsu.joinSideRef('leftJoin), qsu.joinSideRef('rightJoin)), projectFoo),
            qsu._autojoin2(
              qsu.transpose(qsu.map(qsu.joinSideRef('rightJoin), projectCustomersKey), Retain.Values, Rotation.ShiftArray),
              qsu.map(qsu.joinSideRef('rightJoin), projectCustomersKey2),
              func.Add(func.LeftSide, func.RightSide)),
            func.Eq(func.LeftSide, func.RightSide)),
          JoinType.Inner, 'leftJoin, 'rightJoin))

      evaluate(extractFM(graph)) must beLike {
        case \/-(ThetaJoin(
          AutoJoin2(
            Transpose(Read(`orders`), Retain.Values, Rotation.ShiftMap),
            Union(
              Transpose(Read(`orders`), Retain.Values, Rotation.ShiftMap),
              Transpose(Read(`customers`), Retain.Values, Rotation.ShiftMap)),
            autojoinConditionOrders),
          AutoJoin2(
            Transpose(Read(`customers`), Retain.Values, Rotation.ShiftMap),
            Transpose(Map(Transpose(Read(`customers`), Retain.Values, Rotation.ShiftMap), structCustomers), Retain.Values, Rotation.ShiftArray),
            autojoinConditionCustomers),
          on, JoinType.Inner, repair)) => {

          autojoinConditionOrders must beTreeEqual(makeMap("left_source", "left_target_0"))
          autojoinConditionCustomers must beTreeEqual(makeMap("right_source", "right_target_0"))

          repair must beTreeEqual(
            makeMap("left", "right") >>= {
              case LeftSide => func.ProjectKeyS(func.LeftSide, "left_source")
              case RightSide => func.ProjectKeyS(func.RightSide, "right_source")
            })

          on must beTreeEqual(
            func.Eq(
              func.ProjectKeyS(
                func.ProjectKeyS(func.LeftSide, "left_target_0"), "foo"),
              func.Add(
                func.ProjectKeyS(func.RightSide, "right_target_0"),
                func.ProjectKeyS(func.RightSide, "customers_key_2"))))

          structCustomers.linearize must beTreeEqual(projectCustomersKey.linearize)
        }
      }
    }

    "convert a non-mappable join condition on both sides" >> {
      val projectOrdersKey = recProjectStrKey("orders_key")
      val projectCustomersKey = recProjectStrKey("customers_key")

      val graph = QSUGraph.fromTree[Fix](
        qsu.lpJoin(
          qsu.transpose(qsu.read(orders), Retain.Values, Rotation.ShiftMap),
          qsu.transpose(qsu.read(customers), Retain.Values, Rotation.ShiftMap),
          qsu._autojoin2(
            qsu.transpose(qsu.map(qsu.joinSideRef('leftJoin), projectOrdersKey), Retain.Values, Rotation.ShiftArray),
            qsu.transpose(qsu.map(qsu.joinSideRef('rightJoin), projectCustomersKey), Retain.Values, Rotation.ShiftArray),
            func.Eq(func.LeftSide, func.RightSide)),
          JoinType.Inner, 'leftJoin, 'rightJoin))

      evaluate(extractFM(graph)) must beLike {
        case \/-(ThetaJoin(
          AutoJoin2(
            Transpose(Read(`orders`), Retain.Values, Rotation.ShiftMap),
            Transpose(Map(Transpose(Read(`orders`), Retain.Values, Rotation.ShiftMap), structOrders), Retain.Values, Rotation.ShiftArray),
            autojoinConditionOrders),
          AutoJoin2(
            Transpose(Read(`customers`), Retain.Values, Rotation.ShiftMap),
            Transpose(Map(Transpose(Read(`customers`), Retain.Values, Rotation.ShiftMap), structCustomers), Retain.Values, Rotation.ShiftArray),
            autojoinConditionCustomers),
          on, JoinType.Inner, repair)) => {

          autojoinConditionOrders must beTreeEqual(makeMap("left_source", "left_target_0"))
          autojoinConditionCustomers must beTreeEqual(makeMap("right_source", "right_target_0"))

          structOrders.linearize must beTreeEqual(projectOrdersKey.linearize)
          structCustomers.linearize must beTreeEqual(projectCustomersKey.linearize)

          on must beTreeEqual(
            func.Eq(
              func.ProjectKeyS(func.LeftSide, "left_target_0"),
              func.ProjectKeyS(func.RightSide, "right_target_0")))

          repair must beTreeEqual(
            makeMap("left", "right") >>= {
              case LeftSide => func.ProjectKeyS(func.LeftSide, "left_source")
              case RightSide => func.ProjectKeyS(func.RightSide, "right_source")
            })
        }
      }
    }

    "convert a non-mappable join condition on the left" >> {
      val projectOrdersKey = recProjectStrKey("orders_key")
      val projectCustomersKey = recProjectStrKey("customers_key")

      val graph = QSUGraph.fromTree[Fix](
        qsu.lpJoin(
          qsu.transpose(qsu.read(orders), Retain.Values, Rotation.ShiftMap),
          qsu.transpose(qsu.read(customers), Retain.Values, Rotation.ShiftMap),
          qsu._autojoin2(
            qsu.transpose(qsu.map(qsu.joinSideRef('leftJoin), projectOrdersKey), Retain.Values, Rotation.ShiftArray),
            qsu.map(qsu.joinSideRef('rightJoin), projectCustomersKey),
            func.Eq(func.LeftSide, func.RightSide)),
          JoinType.Inner, 'leftJoin, 'rightJoin))

      evaluate(extractFM(graph)) must beLike {
        case \/-(ThetaJoin(
          AutoJoin2(
            Transpose(Read(`orders`), Retain.Values, Rotation.ShiftMap),
            Transpose(Map(Transpose(Read(`orders`), Retain.Values, Rotation.ShiftMap), struct), Retain.Values, Rotation.ShiftArray),
            autojoinCondition),
          Transpose(Read(`customers`), Retain.Values, Rotation.ShiftMap),
          on, JoinType.Inner, repair)) => {

          autojoinCondition must beTreeEqual(makeMap("left_source", "left_target_0"))

          struct.linearize must beTreeEqual(projectOrdersKey.linearize)

          on must beTreeEqual(
            func.Eq(
              func.ProjectKeyS(func.LeftSide, "left_target_0"),
              projectCustomersKey.linearize.as[JoinSide](RightSide)))

          repair must beTreeEqual(
            makeMap("left", "right") >>= {
              case LeftSide => func.ProjectKeyS(func.LeftSide, "left_source")
              case side => Free.pure(side)
            })
        }
      }
    }
  }

  def evaluate[A](fa: F[A]): PlannerError \/ A = fa.run.eval(0L).value
}

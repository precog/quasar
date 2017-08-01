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

package quasar.mimir

import quasar.precog.common._
import quasar.precog.util.IdGen
import quasar.yggdrasil._
import quasar.yggdrasil.bytecode.JType.JUniverseT

import scalaz._

import org.specs2.execute.Result

trait JoinOptimizerSpecs[M[+_]] extends EvaluatorSpecification[M]
    with LongIdMemoryDatasetConsumer[M] { self =>

  import TableModule.CrossOrder._ // TODO: Move CrossOrder out somewhere else.
  import dag._
  import instructions._
  import library.{ op1ForUnOp => _, _ }

  val ctx = defaultEvaluationContext

  object joins extends JoinOptimizer with StdLibOpFinder

  import joins._

  def testEval(graph: DepGraph)(test: Set[SEvent] => Result): Result = {
    (consumeEval(graph, defaultEvaluationContext) match {
      case Success(results) => test(results)
      case Failure(error) => throw error
    })
  }

  "join optimization" should {
    "fail to rewrite in presence of constant join" in {
      val line = Line(1, 1, "")
      val numbers = dag.AbsoluteLoad(Const(CString("/het/numbers"))(line))(line)

      def makeFilter(lhs1: DepGraph, rhs1: DepGraph, lhs2: DepGraph, rhs2: DepGraph) =
        Filter(IdentitySort,
          Join(Eq, Cross(None),
            lhs1,
            rhs1)(line),
          Join(Eq, Cross(None),
            lhs2,
            rhs2)(line))(line)

      val input1 = makeFilter(numbers, numbers, numbers, Const(CLong(13))(line))
      val input2 = makeFilter(numbers, numbers,Const(CLong(13))(line), numbers)
      val input3 = makeFilter(numbers, Const(CLong(13))(line), numbers, numbers)
      val input4 = makeFilter(Const(CLong(13))(line), numbers, numbers, numbers)

      def testInput(input: DepGraph) = {
        val opt = optimizeJoins(input, ctx, new IdGen)
        opt must_== input
      }

      testInput(input1)
      testInput(input2)
      testInput(input3)
      testInput(input4)
    }

    "eliminate naive cartesian products in trivial object join cases" in {

      val rawInput = """
        | a := //users
        | b := //heightWeight
        | a ~ b
        |   { name: a.name, height: b.height } where a.userid = b.userId """.stripMargin

      val line = Line(1, 1, "")
      val users = dag.AbsoluteLoad(Const(CString("/hom/users"))(line), JUniverseT)(line)
      val heightWeight = dag.AbsoluteLoad(Const(CString("/hom/heightWeight"))(line), JUniverseT)(line)
      val height = Const(CString("height"))(line)
      val name = Const(CString("name"))(line)
      val userId = Const(CString("userId"))(line)
      val key = Const(CString("key"))(line)
      val value = Const(CString("value"))(line)

      val liftedLHS =
        AddSortKey(
          Join(JoinObject, IdentitySort,
            Join(WrapObject, Cross(Some(CrossRight)),
              key,
              Join(DerefObject, Cross(None), heightWeight, userId)(line))(line),
            Join(WrapObject, Cross(Some(CrossRight)), value, heightWeight)(line))(line),
          "key", "value", 0)

      val liftedRHS =
        AddSortKey(
          Join(JoinObject, IdentitySort,
            Join(WrapObject, Cross(Some(CrossRight)),
              key,
              Join(DerefObject, Cross(None), users, userId)(line))(line),
            Join(WrapObject, Cross(Some(CrossRight)), value, users)(line))(line),
          "key", "value", 0)

      val input =
        Filter(IdentitySort,
          Join(JoinObject, Cross(None),
            Join(WrapObject, Cross(None),
              height,
              Join(DerefObject, Cross(None), heightWeight, height)(line))(line),
            Join(WrapObject, Cross(None),
              name,
              Join(DerefObject, Cross(None), users, name)(line))(line))(line),
          Join(Eq, Cross(None),
            Join(DerefObject, Cross(None),
              users,
              userId)(line),
            Join(DerefObject, Cross(None),
              heightWeight,
              userId)(line))(line))(line)

      val opt = optimizeJoins(input, ctx, new IdGen)

      val expectedOpt =
        Join(JoinObject, ValueSort(0),
          Join(WrapObject, Cross(None),
            height,
            Join(DerefObject, Cross(None), liftedLHS, height)(line))(line),
          Join(WrapObject, Cross(None),
            name,
            Join(DerefObject, Cross(None), liftedRHS, name)(line))(line))(line)

      opt must_== expectedOpt
    }

    "eliminate naive cartesian products in trivial object join case with non order-preserving op in equal" in {

      val rawInput = """
        | a := //users
        | b := //heightWeight
        | a ~ b
        |   { name: a.name, height: b.height } where std::string::toLowerCase(a.userId) = b.userId """.stripMargin

      val line = Line(1, 1, "")
      val users = dag.AbsoluteLoad(Const(CString("/hom/users"))(line), JUniverseT)(line)
      val heightWeight = dag.AbsoluteLoad(Const(CString("/hom/heightWeight"))(line), JUniverseT)(line)
      val height = Const(CString("height"))(line)
      val name = Const(CString("name"))(line)
      val userId = Const(CString("userId"))(line)
      val key = Const(CString("key"))(line)
      val value = Const(CString("value"))(line)

      val liftedLHS =
        AddSortKey(
          Join(JoinObject, IdentitySort,
            Join(WrapObject, Cross(Some(CrossRight)),
              key,
              Join(DerefObject, Cross(None), heightWeight, userId)(line))(line),
            Join(WrapObject, Cross(Some(CrossRight)), value, heightWeight)(line))(line),
          "key", "value", 0)

      val liftedRHS =
        AddSortKey(
          Join(JoinObject, IdentitySort,
            Join(WrapObject, Cross(Some(CrossRight)),
              key,
              Operate(BuiltInFunction1Op(toLowerCase),
                Join(DerefObject, Cross(None), users, userId)(line))(line))(line),
            Join(WrapObject, Cross(Some(CrossRight)), value, users)(line))(line),
          "key", "value", 0)

      val input =
        Filter(IdentitySort,
          Join(JoinObject, Cross(None),
            Join(WrapObject, Cross(None),
              height,
              Join(DerefObject, Cross(None), heightWeight, height)(line))(line),
            Join(WrapObject, Cross(None),
              name,
              Join(DerefObject, Cross(None), users, name)(line))(line))(line),
          Join(Eq, Cross(None),
            Operate(BuiltInFunction1Op(toLowerCase),
              Join(DerefObject, Cross(None),
                users,
                userId)(line))(line),
            Join(DerefObject, Cross(None),
              heightWeight,
              userId)(line))(line))(line)

      val opt = optimizeJoins(input, ctx, new IdGen)

      val expectedOpt =
        Join(JoinObject, ValueSort(0),
          Join(WrapObject, Cross(None),
            height,
            Join(DerefObject, Cross(None), liftedLHS, height)(line))(line),
          Join(WrapObject, Cross(None),
            name,
            Join(DerefObject, Cross(None), liftedRHS, name)(line))(line))(line)

      opt must_== expectedOpt
    }

    "eliminate naive cartesian products in trivial op2 join cases" in {

      val rawInput = """
        | a := //users
        | b := //heightWeight
        | a ~ b
        |   std::math::hypot(a.weight, b.height) where a.userid = b.userId """.stripMargin

      val line = Line(1, 1, "")
      val users = dag.AbsoluteLoad(Const(CString("/hom/users"))(line), JUniverseT)(line)
      val heightWeight = dag.AbsoluteLoad(Const(CString("/hom/heightWeight"))(line), JUniverseT)(line)
      val height = Const(CString("height"))(line)
      val weight = Const(CString("weight"))(line)
      val userId = Const(CString("userId"))(line)
      val key = Const(CString("key"))(line)
      val value = Const(CString("value"))(line)

      val liftedLHS =
        AddSortKey(
          Join(JoinObject, IdentitySort,
            Join(WrapObject, Cross(Some(CrossRight)),
              key,
              Join(DerefObject, Cross(None), users, userId)(line))(line),
            Join(WrapObject, Cross(Some(CrossRight)), value, users)(line))(line),
          "key", "value", 0)

      val liftedRHS =
        AddSortKey(
          Join(JoinObject, IdentitySort,
            Join(WrapObject, Cross(Some(CrossRight)),
              key,
              Join(DerefObject, Cross(None), heightWeight, userId)(line))(line),
            Join(WrapObject, Cross(Some(CrossRight)), value, heightWeight)(line))(line),
          "key", "value", 0)

      val input =
        Filter(IdentitySort,
          Join(BuiltInFunction2Op(hypot), Cross(None),
            Join(DerefObject, Cross(None), users, weight)(line),
            Join(DerefObject, Cross(None), heightWeight, height)(line))(line),
          Join(Eq, Cross(None),
            Join(DerefObject, Cross(None),
              users,
              userId)(line),
            Join(DerefObject, Cross(None),
              heightWeight,
              userId)(line))(line))(line)

      val opt = optimizeJoins(input, ctx, new IdGen)

      val expectedOpt =
        Join(BuiltInFunction2Op(hypot), ValueSort(0),
          Join(DerefObject, Cross(None), liftedLHS, weight)(line),
          Join(DerefObject, Cross(None), liftedRHS, height)(line))(line)

      opt must_== expectedOpt
    }

    "eliminate naive cartesian products in object join followed by object deref" in {

      val rawInput = """
        | a := //users
        | b := //heightWeight
        | a ~ b
        |   std::string::toLowerCase({ name: a.name, height: b.height }.height) where a.userId = b.userId
        """.stripMargin

      val line = Line(1, 1, "")
      val users = dag.AbsoluteLoad(Const(CString("/hom/users"))(line), JUniverseT)(line)
      val heightWeight = dag.AbsoluteLoad(Const(CString("/hom/heightWeight"))(line), JUniverseT)(line)
      val height = Const(CString("height"))(line)
      val name = Const(CString("name"))(line)
      val userId = Const(CString("userId"))(line)
      val key = Const(CString("key"))(line)
      val value = Const(CString("value"))(line)

      val liftedLHS =
        AddSortKey(
          Join(JoinObject, IdentitySort,
            Join(WrapObject, Cross(Some(CrossRight)),
              key,
              Join(DerefObject, Cross(None), heightWeight, userId)(line))(line),
            Join(WrapObject, Cross(Some(CrossRight)), value, heightWeight)(line))(line),
          "key", "value", 0)

      val liftedRHS =
        AddSortKey(
          Join(JoinObject, IdentitySort,
            Join(WrapObject, Cross(Some(CrossRight)),
              key,
              Join(DerefObject, Cross(None), users, userId)(line))(line),
            Join(WrapObject, Cross(Some(CrossRight)), value, users)(line))(line),
          "key", "value", 0)

      val input =
        Filter(IdentitySort,
          Operate(BuiltInFunction1Op(toLowerCase),
            Join(DerefObject, Cross(None),
              Join(JoinObject, Cross(None),
                Join(WrapObject, Cross(None),
                  height,
                  Join(DerefObject, Cross(None), heightWeight, height)(line))(line),
                Join(WrapObject, Cross(None),
                  name,
                  Join(DerefObject, Cross(None), users, name)(line))(line))(line),
              name)(line))(line),
          Join(Eq, Cross(None),
            Join(DerefObject, Cross(None),
              users,
              userId)(line),
            Join(DerefObject, Cross(None),
              heightWeight,
              userId)(line))(line))(line)

      val opt = optimizeJoins(input, ctx, new IdGen)

      val expectedOpt =
        Operate(BuiltInFunction1Op(toLowerCase),
          Join(DerefObject, Cross(None),
            Join(JoinObject, ValueSort(0),
              Join(WrapObject, Cross(None),
                height,
                Join(DerefObject, Cross(None), liftedLHS, height)(line))(line),
              Join(WrapObject, Cross(None),
                name,
                Join(DerefObject, Cross(None), liftedRHS, name)(line))(line))(line),
            name)(line))(line)

      opt must_== expectedOpt
    }

    "fail to eliminate join where lhs and rhs originate from same place" in {
      val rawInput = """
        | a := //users
        | b := //heightWeight
        | a ~ b
        |   [a, a] where a = b
        """.stripMargin

      val line = Line(1, 1, "")
      val users = dag.AbsoluteLoad(Const(CString("/hom/users"))(line), JUniverseT)(line)
      val heightWeight = dag.AbsoluteLoad(Const(CString("/hom/heightWeight"))(line), JUniverseT)(line)

      val input =
        Filter(IdentitySort,
          Join(JoinArray, Cross(None),
            Operate(WrapArray, users)(line),
            Operate(WrapArray, users)(line))(line),
          Join(Eq, Cross(None), users, heightWeight)(line))(line)

      val opt = optimizeJoins(input, ctx, new IdGen)

      opt must_== input
    }

    "fail to eliminate cartesian products wrapped in non row-level transformation" in {

      val rawInput = """
        | a := //users
        | b := //heightWeight
        | a ~ b
        |   flatten({ name: a.name, height: b.height }.height) where a.userId = b.userId
        """.stripMargin

      val line = Line(1, 1, "")
      val users = dag.AbsoluteLoad(Const(CString("/hom/users"))(line), JUniverseT)(line)
      val heightWeight = dag.AbsoluteLoad(Const(CString("/hom/heightWeight"))(line), JUniverseT)(line)
      val height = Const(CString("height"))(line)
      val name = Const(CString("name"))(line)
      val userId = Const(CString("userId"))(line)
      val key = Const(CString("key"))(line)
      val value = Const(CString("value"))(line)

      val input =
        Filter(IdentitySort,
          dag.Morph1(Flatten,
            Join(DerefObject, Cross(None),
              Join(JoinObject, Cross(None),
                Join(WrapObject, Cross(None),
                  height,
                  Join(DerefObject, Cross(None), heightWeight, height)(line))(line),
                Join(WrapObject, Cross(None),
                  name,
                  Join(DerefObject, Cross(None), users, name)(line))(line))(line),
              name)(line))(line),
          Join(Eq, Cross(None),
            Join(DerefObject, Cross(None),
              users,
              userId)(line),
            Join(DerefObject, Cross(None),
              heightWeight,
              userId)(line))(line))(line)

      val opt = optimizeJoins(input, ctx, new IdGen)

      opt must_== input
    }

    "eliminate naive cartesian products in less trivial object join cases" in {

      val rawInput = """
        | clicks := //clicks
        | conversions := //conversions
        |
        | clicks ~ conversions
        | {clicks: clicks,  price : conversions.product.ID} where conversions.product.price = clicks.product.price
      """.stripMargin

      val line = Line(1, 1, "")
      val clicksData = dag.AbsoluteLoad(Const(CString("/clicks"))(line), JUniverseT)(line)
      val conversionsData = dag.AbsoluteLoad(Const(CString("/conversions"))(line), JUniverseT)(line)
      val clicks = Const(CString("clicks"))(line)
      val price = Const(CString("price"))(line)
      val id = Const(CString("ID"))(line)
      val product = Const(CString("product"))(line)
      val key = Const(CString("key"))(line)
      val value = Const(CString("value"))(line)

      val liftedLHS =
        AddSortKey(
          Join(JoinObject, IdentitySort,
            Join(WrapObject, Cross(Some(CrossRight)),
              key,
              Join(DerefObject,Cross(None),
                Join(DerefObject,Cross(None),
                  clicksData,
                  product)(line),
                price)(line))(line),
            Join(WrapObject, Cross(Some(CrossRight)), value, clicksData)(line))(line),
          "key", "value", 0)

      val liftedRHS =
        AddSortKey(
          Join(JoinObject, IdentitySort,
            Join(WrapObject, Cross(Some(CrossRight)),
              key,
              Join(DerefObject,Cross(None),
                Join(DerefObject,Cross(None),
                  conversionsData,
                  product)(line),
                price)(line))(line),
            Join(WrapObject, Cross(Some(CrossRight)), value, conversionsData)(line))(line),
          "key", "value", 0)

      val input =
        Filter(IdentitySort,
          Join(JoinObject,Cross(None),
            Join(WrapObject,Cross(None),
              clicks,
              clicksData)(line),
            Join(WrapObject,Cross(None),
              price,
              Join(DerefObject,Cross(None),
                Join(DerefObject,Cross(None),
                  conversionsData,
                  product)(line),
                id)(line))(line))(line),
          Join(Eq,Cross(None),
            Join(DerefObject,Cross(None),
              Join(DerefObject,Cross(None),
                conversionsData,
                product)(line),
              price)(line),
            Join(DerefObject,Cross(None),
              Join(DerefObject,Cross(None),
                clicksData,
                product)(line),
              price)(line))(line))(line)

      val opt = optimizeJoins(input, ctx, new IdGen)

      val expectedOpt =
        Join(JoinObject, ValueSort(0),
          Join(WrapObject, Cross(None),
            clicks,
            liftedLHS)(line),
          Join(WrapObject, Cross(None),
            price,
            Join(DerefObject, Cross(None),
              Join(DerefObject, Cross(None),
                liftedRHS,
                product)(line),
              id)(line))(line))(line)


      opt must_== expectedOpt
    }

    "eliminate naive cartesian products in trivial array join cases" in {

      val rawInput = """
        | a := //users
        | b := //heightWeight
        | a ~ b
        |   [b.height, a.name] where a.userId = b.userId """.stripMargin

      val line = Line(1, 1, "")
      val users = dag.AbsoluteLoad(Const(CString("/hom/users"))(line), JUniverseT)(line)
      val heightWeight = dag.AbsoluteLoad(Const(CString("/hom/heightWeight"))(line), JUniverseT)(line)
      val height = Const(CString("height"))(line)
      val name = Const(CString("name"))(line)
      val userId = Const(CString("userId"))(line)
      val key = Const(CString("key"))(line)
      val value = Const(CString("value"))(line)

      val liftedLHS =
        AddSortKey(
          Join(JoinObject, IdentitySort,
            Join(WrapObject, Cross(Some(CrossRight)),
              key,
              Join(DerefObject, Cross(None), heightWeight, userId)(line))(line),
            Join(WrapObject, Cross(Some(CrossRight)), value, heightWeight)(line))(line),
          "key", "value", 0)

      val liftedRHS =
        AddSortKey(
          Join(JoinObject, IdentitySort,
            Join(WrapObject, Cross(Some(CrossRight)),
              key,
              Join(DerefObject, Cross(None), users, userId)(line))(line),
            Join(WrapObject, Cross(Some(CrossRight)), value, users)(line))(line),
          "key", "value", 0)

      val input =
        Filter(IdentitySort,
          Join(JoinArray, Cross(None),
            Operate(WrapArray,
              Join(DerefObject, Cross(None), heightWeight, height)(line))(line),
            Operate(WrapArray,
              Join(DerefObject, Cross(None), users, name)(line))(line))(line),
          Join(Eq, Cross(None),
            Join(DerefObject, Cross(None),
              users,
              userId)(line),
            Join(DerefObject, Cross(None),
              heightWeight,
              userId)(line))(line))(line)

      val opt = optimizeJoins(input, ctx, new IdGen)

      val expectedOpt =
        Join(JoinArray, ValueSort(0),
          Operate(WrapArray,
            Join(DerefObject, Cross(None), liftedLHS, height)(line))(line),
          Operate(WrapArray,
            Join(DerefObject, Cross(None), liftedRHS, name)(line))(line))(line)

      opt must_== expectedOpt
    }

    "eliminate naive cartesian products in slightly less trivial cases (1)" in {

      val rawInput = """
        | a := //users
        | b := //heightWeight
        | a ~ b
        |   { name: a.name, height: b.height, weight: b.weight } where a.userId = b.userId """.stripMargin

      val line = Line(1, 1, "")
      val users = dag.AbsoluteLoad(Const(CString("/users"))(line))(line)
      val heightWeight = dag.AbsoluteLoad(Const(CString("/heightWeight"))(line))(line)
      val userId = Const(CString("userId"))(line)
      val name = Const(CString("name"))(line)
      val height = Const(CString("height"))(line)
      val weight = Const(CString("weight"))(line)
      val key = Const(CString("key"))(line)
      val value = Const(CString("value"))(line)

      def makeAddSortKey(key: DepGraph, value: DepGraph) =
        AddSortKey(
          Join(JoinObject, IdentitySort,
            Join(WrapObject, Cross(Some(CrossRight)),
              Const(CString("key"))(line),
              key)(line),
            Join(WrapObject, Cross(Some(CrossRight)),
              Const(CString("value"))(line),
              value)(line))(line),
          "key", "value", 0)

      val liftedRHS =
        AddSortKey(
          Join(JoinObject, IdentitySort,
            Join(WrapObject, Cross(Some(CrossRight)),
              key,
              Join(DerefObject, Cross(None), users, userId)(line))(line),
            Join(WrapObject, Cross(Some(CrossRight)), value, users)(line))(line),
          "key", "value", 0)

      val lhs =
        Join(JoinObject, IdentitySort,
          Join(WrapObject, Cross(None),
            height,
            Join(DerefObject, Cross(None), heightWeight, height)(line))(line),
          Join(WrapObject, Cross(None),
            weight,
            Join(DerefObject, Cross(None), heightWeight, weight)(line))(line))(line)


      val input =
        Filter(IdentitySort,
          Join(JoinObject, Cross(None),
            lhs,
            Join(WrapObject, Cross(None),
              name,
              Join(DerefObject, Cross(None), users, name)(line))(line))(line),
          Join(Eq, Cross(None),
            Join(DerefObject, Cross(None), users, userId)(line),
            Join(DerefObject, Cross(None), heightWeight, userId)(line))(line))(line)

      val hwid = Join(DerefObject, Cross(None), heightWeight, userId)(line)
      val opt = optimizeJoins(input, ctx, new IdGen)

      val expectedOpt =
        Join(JoinObject, ValueSort(0),
          makeAddSortKey(hwid, lhs),
          Join(WrapObject, Cross(None),
            name,
            Join(DerefObject, Cross(None), liftedRHS, name)(line))(line))(line)

       opt must_== expectedOpt
    }

    "eliminate naive cartesian products in slightly less trivial cases (2)" in {

      val rawInput = """
        | a := //users
        | b := //heightWeight
        | a ~ b
        |   ({ name: a.name } with b) where a.userId = b.userId """.stripMargin

      val line = Line(1, 1, "")

      lazy val users = dag.AbsoluteLoad(Const(CString("/users"))(line))(line)
      lazy val heightWeight = dag.AbsoluteLoad(Const(CString("/heightWeight"))(line))(line)
      lazy val userId = Const(CString("userId"))(line)
      lazy val name = Const(CString("name"))(line)
      lazy val key = Const(CString("key"))(line)
      lazy val value = Const(CString("value"))(line)

      lazy val input =
        Filter(IdentitySort,
          Join(JoinObject, Cross(None),
            Join(WrapObject, Cross(None),
              name,
              Join(DerefObject, Cross(None), users, name)(line)
            )(line),
            heightWeight
          )(line),
          Join(Eq, Cross(None),
            Join(DerefObject, Cross(None), users, userId)(line),
            Join(DerefObject, Cross(None), heightWeight, userId)(line)
          )(line)
        )(line)

      val opt = optimizeJoins(input, ctx, new IdGen)

      val expectedOpt =
        Join(JoinObject, ValueSort(0),
          Join(WrapObject, Cross(None),
            name,
            Join(DerefObject, Cross(None),
              AddSortKey(
                Join(JoinObject, IdentitySort,
                  Join(WrapObject, Cross(Some(CrossRight)),
                    key,
                    Join(DerefObject, Cross(None), users, userId)(line))(line),
                  Join(WrapObject, Cross(Some(CrossRight)), value, users)(line))(line),
                "key", "value", 0),
              name)(line))(line),
          AddSortKey(
            Join(JoinObject, IdentitySort,
              Join(WrapObject, Cross(Some(CrossRight)),
                key,
                Join(DerefObject, Cross(None), heightWeight, userId)(line))(line),
              Join(WrapObject, Cross(Some(CrossRight)), value, heightWeight)(line))(line),
            "key", "value", 0))(line)

      opt must_== expectedOpt
    }

    "eliminate cartesian with a new inside an array join" in {
      val query = """
        | medals' := //summer_games/london_medals
        | medals'' := new medals'
        |
        | medals'' ~ medals'
        | [medals'.Name, medals''.Name] where medals'.Name = medals''.Name
      """.stripMargin

      val line = Line(1, 1, "")

      val name = Const(CString("Name"))(line)
      val medals = dag.AbsoluteLoad(Const(CString("/summer_games/london_medals"))(line))(line)
      val newMedals = dag.New(medals)(line)
      val key = Const(CString("key"))(line)
      val value = Const(CString("value"))(line)

      val input = Filter(IdentitySort,
        Join(ArraySwap,Cross(None),
          Join(JoinArray,Cross(None),
            Operate(WrapArray,
              Join(DerefObject,Cross(None),
                newMedals,
                name)(line))(line),
            Operate(WrapArray,
              Join(DerefObject,Cross(None),
                medals,
                name)(line))(line))(line),
          Const(CLong(1))(line))(line),
        Join(Eq,Cross(None),
          Join(DerefObject,Cross(None),
            medals,
            name)(line),
          Join(DerefObject,Cross(None),
            newMedals,
            name)(line))(line))(line)

      val liftedLHS =
        AddSortKey(
          Join(JoinObject, IdentitySort,
            Join(WrapObject, Cross(Some(CrossRight)),
              key,
              Join(DerefObject, Cross(None), medals, name)(line))(line),
            Join(WrapObject, Cross(Some(CrossRight)), value, medals)(line))(line),
          "key", "value", 0)

      val liftedRHS =
        AddSortKey(
          Join(JoinObject, IdentitySort,
            Join(WrapObject, Cross(Some(CrossRight)),
              key,
              Join(DerefObject, Cross(None), newMedals, name)(line))(line),
            Join(WrapObject, Cross(Some(CrossRight)), value, newMedals)(line))(line),
          "key", "value", 0)

      // `liftedRHS` and `liftedLHS` are swapped because of the `ArraySwap`
      val expectedOpt = Join(ArraySwap, Cross(None),
        Join(JoinArray, ValueSort(0),
          Operate(WrapArray,
            Join(DerefObject, Cross(None),
              liftedRHS,
              name)(line))(line),
          Operate(WrapArray,
            Join(DerefObject, Cross(None),
              liftedLHS,
              name)(line))(line))(line),
        Const(CLong(1))(line))(line)

      val opt = optimizeJoins(input, ctx, new IdGen)

      opt must_== expectedOpt
    }

    "eliminate cartesian with a new and a filter inside an array join" in {
      val query = """
        | medals := //summer_games/london_medals
        | medals' := medals where medals.Country = "India"
        | medals'' := new medals''
        |
        | medals'' ~ medals'
        | [medals'.Country, medals''.Country] where medals'.Total = medals''.Total
      """.stripMargin

      val line = Line(1, 1, "")

      val country = Const(CString("Country"))(line)
      val total = Const(CString("Total"))(line)
      val medals = dag.AbsoluteLoad(Const(CString("/summer_games/london_medals"))(line))(line)
      val medalsP =
        dag.Filter(IdentitySort,
          medals,
          Join(Eq, Cross(None),
            Join(DerefObject, Cross(None), medals, country)(line),
            Const(CString("India"))(line))(line))(line)
      val medalsPP = dag.New(medalsP)(line)
      val medalsPcountry = Join(DerefObject,Cross(None), medalsP, country)(line)
      val medalsPPcountry = Join(DerefObject,Cross(None), medalsPP, country)(line)
      val medalsPtotal = Join(DerefObject,Cross(None), medalsP, total)(line)
      val medalsPPtotal = Join(DerefObject,Cross(None), medalsPP, total)(line)
      val key = Const(CString("key"))(line)
      val value = Const(CString("value"))(line)

      val input = Filter(IdentitySort,
        Join(JoinArray,Cross(None),
          Operate(WrapArray,
            medalsPcountry)(line),
          Operate(WrapArray,
            medalsPPcountry)(line))(line),
        Join(Eq,Cross(None),
          medalsPtotal,
          medalsPPtotal)(line))(line)

      val liftedLHS =
        AddSortKey(
          Join(JoinObject, IdentitySort,
            Join(WrapObject, Cross(Some(CrossRight)),
              key,
              medalsPtotal)(line),
            Join(WrapObject, Cross(Some(CrossRight)), value, medalsP)(line))(line),
          "key", "value", 0)

      val liftedRHS =
        AddSortKey(
          Join(JoinObject, IdentitySort,
            Join(WrapObject, Cross(Some(CrossRight)),
              key,
              medalsPPtotal)(line),
            Join(WrapObject, Cross(Some(CrossRight)), value, medalsPP)(line))(line),
          "key", "value", 0)

      val expectedOpt =
        Join(JoinArray, ValueSort(0),
          Operate(WrapArray,
            Join(DerefObject, Cross(None),
              liftedLHS,
              country)(line))(line),
          Operate(WrapArray,
            Join(DerefObject, Cross(None),
              liftedRHS,
              country)(line))(line))(line)

      val opt = optimizeJoins(input, ctx, new IdGen)

      opt must_== expectedOpt
    }

    "eliminate cartesian in medal winners query" in {
      // note: query will not return expected results on actual data because
      // `medals.Name` and `athletes.Name` return (First Last) and (Last First), resp.

      /*
       * import std::string::toLowerCase
       *
       * medals := //summer_games/london_medals
       * athletes := //summer_games/athletes
       *
       * medals' := medals with { name: toLowerCase(medals.name) }
       * athletes' := athletes with { name: toLowerCase(athletes.name) }
       *
       * medals' ~ athletes'
       *   { winner: medals'.`Medal winner`, country: athletes'.Countryname }
       *     where medals'.name = athletes'.name
       */

      val line = Line(1, 1, "")

      val medals = dag.AbsoluteLoad(Const(CString("/summer_games/london_medals"))(line))(line)
      val athletes = dag.AbsoluteLoad(Const(CString("/summer_games/athletes"))(line))(line)

      def wrapName(graph: DepGraph) =
        Join(WrapObject, Cross(Some(CrossRight)),
          Const(CString("name"))(line),
          Operate(BuiltInFunction1Op(toLowerCase),
            Join(DerefObject, Cross(None), graph, Const(CString("name"))(line))(line))(line))(line)

      val medalsPrime = Join(JoinObject, IdentitySort,
        medals,
        wrapName(medals))(line)

      val athletesPrime = Join(JoinObject, IdentitySort,
        athletes,
        wrapName(athletes))(line)

      val input = Filter(IdentitySort,
        Join(JoinObject, Cross(None),
          Join(WrapObject, Cross(Some(CrossRight)),
            Const(CString("winner"))(line),
            Join(DerefObject, Cross(None),
              medalsPrime,
              Const(CString("Medal winner"))(line))(line))(line),
          Join(WrapObject, Cross(Some(CrossRight)),
            Const(CString("country"))(line),
            Join(DerefObject, Cross(None),
              athletesPrime,
              Const(CString("Countryname"))(line))(line))(line))(line),
        Join(Eq, Cross(None),
          Join(DerefObject, Cross(None),
            medalsPrime,
            Const(CString("name"))(line))(line),
          Join(DerefObject, Cross(None),
            athletesPrime,
            Const(CString("name"))(line))(line))(line))(line)

      val liftedLHS =
        AddSortKey(
          Join(JoinObject, IdentitySort,
            Join(WrapObject, Cross(Some(CrossRight)),
              Const(CString("key"))(line),
              Join(DerefObject, Cross(None),
                medalsPrime,
                Const(CString("name"))(line))(line))(line),
            Join(WrapObject, Cross(Some(CrossRight)),
              Const(CString("value"))(line),
              medalsPrime)(line))(line),
          "key", "value", 0)

      val liftedRHS =
        AddSortKey(
          Join(JoinObject, IdentitySort,
            Join(WrapObject, Cross(Some(CrossRight)),
              Const(CString("key"))(line),
              Join(DerefObject, Cross(None),
                athletesPrime,
                Const(CString("name"))(line))(line))(line),
            Join(WrapObject, Cross(Some(CrossRight)),
              Const(CString("value"))(line),
              athletesPrime)(line))(line),
          "key", "value", 0)

      val result = optimizeJoins(input, ctx, new IdGen)

      val expected =
        Join(JoinObject, ValueSort(0),
          Join(WrapObject, Cross(Some(CrossRight)),
            Const(CString("winner"))(line),
            Join(DerefObject, Cross(None),
              liftedLHS,
              Const(CString("Medal winner"))(line))(line))(line),
          Join(WrapObject, Cross(Some(CrossRight)),
            Const(CString("country"))(line),
            Join(DerefObject, Cross(None),
              liftedRHS,
              Const(CString("Countryname"))(line))(line))(line))(line)

      result mustEqual expected
    }

    "produce a valid dag for a ternary object-literal cartesian" in {
      /*
       * clicks := //clicks
       *
       * clicks' := new clicks
       * clicks ~ clicks'
       *   { a: clicks, b: clicks', c: clicks } where clicks'.pageId = clicks.pageId
       */

      val line = Line(1, 1, "")

      lazy val clicks = dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line)
      lazy val clicksP = dag.New(clicks)(line)

      lazy val input =
        Filter(IdentitySort,
          Join(JoinObject, Cross(None),
            Join(JoinObject, IdentitySort,
              Join(WrapObject, Cross(None),
                Const(CString("a"))(line),
                clicks)(line),
              Join(WrapObject, Cross(None),
                Const(CString("c"))(line),
                clicks)(line))(line),
            Join(WrapObject, Cross(None),
              Const(CString("b"))(line),
              clicksP)(line))(line),
          Join(Eq, Cross(None),
            Join(DerefObject, Cross(None),
              clicks,
              Const(CString("pageId"))(line))(line),
            Join(DerefObject, Cross(None),
              clicksP,
              Const(CString("pageId"))(line))(line))(line))(line)

        def makeAddSortKey(key: DepGraph, value: DepGraph) =
          AddSortKey(
            Join(JoinObject, IdentitySort,
              Join(WrapObject, Cross(Some(CrossRight)),
                Const(CString("key"))(line),
                key)(line),
              Join(WrapObject, Cross(Some(CrossRight)),
                Const(CString("value"))(line),
                value)(line))(line),
            "key", "value", 0)

        val clicksid = Join(DerefObject, Cross(None), clicks, Const(CString("pageId"))(line))(line)
        val clicksPid = Join(DerefObject, Cross(None), clicksP, Const(CString("pageId"))(line))(line)

        val clicksV =
          Join(JoinObject, IdentitySort,
            Join(WrapObject, Cross(None),
              Const(CString("a"))(line),
              clicks)(line),
            Join(WrapObject, Cross(None),
              Const(CString("c"))(line),
              clicks)(line))(line)

        lazy val expected =
          Join(JoinObject, ValueSort(0),
            makeAddSortKey(clicksid, clicksV),
            Join(WrapObject, Cross(None),
              Const(CString("b"))(line),
              makeAddSortKey(clicksPid, clicksP))(line))(line)

      optimizeJoins(input, ctx, new IdGen) mustEqual expected
    }
  }
}

object JoinOptimizerSpecs extends JoinOptimizerSpecs[Need]

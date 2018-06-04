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

package quasar.mimir

import quasar.precog.common._
import quasar.precog.util.Identifier
import quasar.yggdrasil._

trait ReductionFinderSpecs extends EvaluatorSpecification {

  import instructions._
  import dag._
  import library.{ op1ForUnOp => _, _ }
  import TableModule.CrossOrder._

  val evalCtx = defaultEvaluationContext

  object reductions extends ReductionFinder with StdLibOpFinder

  import reductions._

  "mega reduce" should {

    "in a load, rewrite to itself" in {
      val input = dag.AbsoluteLoad(Const(CString("/foo")))

      megaReduce(input, findReductions(input, evalCtx)) mustEqual input
    }

    "in a reduction of a singleton" in {
      val input = dag.Reduce(Count, Const(CString("alpha")))
      val megaR = dag.MegaReduce(List((trans.Leaf(trans.Source), List(input.red))), Const(CString("alpha")))

      val expected = joinDeref(megaR, 0, 0)

      megaReduce(input, findReductions(input, evalCtx)) mustEqual expected
    }

    "in a single reduction" in {
      val input = dag.Reduce(Count,
        dag.AbsoluteLoad(Const(CString("/foo"))))


      val parent = dag.AbsoluteLoad(Const(CString("/foo")))
      val red = Count
      val megaR = dag.MegaReduce(List((trans.Leaf(trans.Source), List(red))), parent)

      val expected = joinDeref(megaR, 0, 0)

      megaReduce(input, findReductions(input, evalCtx)) mustEqual expected
    }

    "in joins where transpecs are eq, wrap object, operate, filter" in {
      val clicks = dag.AbsoluteLoad(Const(CString("/clicks")))

      val notEq = Join(NotEq, Cross(None),
        Join(DerefObject, Cross(None),
          clicks,
          Const(CString("foo"))),
        Const(CNum(5)))

      val obj = Join(WrapObject, Cross(None),
        Const(CString("bar")),
        clicks)

      val op = Operate(Neg,
        Join(DerefArray, Cross(None),
          clicks,
          Const(CNum(1))))

      val filter = Filter(IdentitySort,
        clicks,
        Join(Eq, Cross(None),
          Join(DerefObject, Cross(None),
            clicks,
            Const(CString("baz"))),
          Const(CNum(12))))

      val fooDerefTrans = trans.DerefObjectStatic(trans.Leaf(trans.Source), CPathField("foo"))
      val nonEqTrans = trans.Map1(trans.Equal(fooDerefTrans, trans.ConstLiteral(CNum(5), fooDerefTrans)), Unary.Comp.f1)
      val objTrans = trans.WrapObject(trans.Leaf(trans.Source), "bar")
      val opTrans = op1ForUnOp(Neg).spec(trans.DerefArrayStatic(trans.Leaf(trans.Source), CPathIndex(1)))
      val bazDerefTrans = trans.DerefObjectStatic(trans.Leaf(trans.Source), CPathField("baz"))
      val filterTrans = trans.Filter(trans.Leaf(trans.Source), trans.Equal(bazDerefTrans, trans.ConstLiteral(CNum(12), bazDerefTrans)))

      val reductions: List[(trans.TransSpec1, List[Reduction])] = List((filterTrans, List(StdDev)), (opTrans, List(Max)), (objTrans, List(Max, Sum)), (nonEqTrans, List(Min))).reverse
      val megaR = MegaReduce(reductions, clicks)

      val input = Join(Sub, Cross(None),
        dag.Reduce(Min, notEq),
        Join(Sub, Cross(None),
          dag.Reduce(Max, obj),
          Join(Sub, Cross(None),
            dag.Reduce(Max, op),
            Join(Sub, Cross(None),
              dag.Reduce(StdDev, filter),
              dag.Reduce(Sum, obj)))))

      val expected = Join(Sub, Cross(None),
        joinDeref(megaR, 3, 0),
        Join(Sub, Cross(None),
          joinDeref(megaR, 2, 1),
          Join(Sub, Cross(None),
            joinDeref(megaR, 1, 0),
            Join(Sub, Cross(None),
              joinDeref(megaR, 0, 0),
              joinDeref(megaR, 2, 0)))))

      megaReduce(input, findReductions(input, evalCtx)) mustEqual expected
    }

    "in a join of two reductions on the same dataset" in {
      val parent = dag.AbsoluteLoad(Const(CString("/foo")))
      val red1 = Count
      val red2 = StdDev
      val left = dag.Reduce(red1, parent)
      val right = dag.Reduce(red2, parent)

      val input = Join(Add, Cross(None), left, right)

      val reductions = List((trans.Leaf(trans.Source), List(red1, red2)))
      val megaR = dag.MegaReduce(reductions, parent)

      val expected = Join(Add, Cross(None),
        joinDeref(megaR, 0, 1),
        joinDeref(megaR, 0, 0))

      val expectedReductions = MegaReduceState(
        Map(left -> parent, right -> parent),
        Map(parent -> List(parent)),
        Map(parent -> List(left, right)),
        Map(parent -> trans.Leaf(trans.Source))
      )

      findReductions(input, evalCtx) mustEqual expectedReductions

      megaReduce(input, findReductions(input, evalCtx)) mustEqual expected


    }

    "in a join where only one side is a reduction" in {
      val load = dag.AbsoluteLoad(Const(CString("/foo")))
      val reduction = StdDev
      val r = dag.Reduce(reduction, load)

      val spec = trans.Leaf(trans.Source)
      val megaR = dag.MegaReduce(List((spec, List(reduction))), load)

      "right" in {
        val input = Join(Add, Cross(None), dag.Operate(Neg, load), r)
        val expected = Join(Add, Cross(None),
          dag.Operate(Neg, load),
          joinDeref(megaR, 0, 0))

        megaReduce(input, findReductions(input, evalCtx)) mustEqual expected
      }
      "left" in {
        val input = Join(Add, Cross(None), r, dag.Operate(Neg, load))
        val expected = Join(Add, Cross(None),
          joinDeref(megaR, 0, 0),
          dag.Operate(Neg, load))

        megaReduce(input, findReductions(input, evalCtx)) mustEqual expected
      }
    }

    "where two different sets are being reduced" in {
      val load1 = dag.AbsoluteLoad(Const(CString("/foo")))
      val load2 = dag.AbsoluteLoad(Const(CString("/bar")))

      val red = Count
      val r1 = dag.Reduce(red, load1)
      val r2 = dag.Reduce(red, load2)

      val input = Join(Add, Cross(Some(CrossRight)), r1, r2)
      val spec = trans.Leaf(trans.Source)

      val megaR1 = dag.MegaReduce(List((spec, List(red))), load1)
      val megaR2 = dag.MegaReduce(List((spec, List(red))), load2)

      val expected = Join(Add, Cross(Some(CrossRight)),
        joinDeref(megaR1, 0, 0),
        joinDeref(megaR2, 0, 0))

      megaReduce(input, findReductions(input, evalCtx)) mustEqual expected
    }

    "where two different sets are being reduced" in {
      val load1 = dag.AbsoluteLoad(Const(CString("/foo")))
      val load2 = dag.AbsoluteLoad(Const(CString("/bar")))

      val r1 = dag.Reduce(Sum, load1)
      val r2 = dag.Reduce(Max, load1)
      val r3 = dag.Reduce(Count, load2)
      val r4 = dag.Reduce(Max, load2)
      val r5 = dag.Reduce(Min, load2)

      val input = Join(Add, Cross(Some(CrossRight)),
        r1,
        Join(Add, Cross(Some(CrossRight)),
          r2,
          Join(Add, Cross(Some(CrossRight)),
            r3,
            Join(Add, Cross(Some(CrossRight)),
              r4,
              r5))))

      val spec = trans.Leaf(trans.Source)

      val megaR1 = dag.MegaReduce(List((spec, List(Sum, Max))), load1)
      val megaR2 = dag.MegaReduce(List((spec, List(Count, Max, Min))), load2)

      val expected = Join(Add, Cross(Some(CrossRight)),
        joinDeref(megaR1, 0, 1),
        Join(Add, Cross(Some(CrossRight)),
          joinDeref(megaR1, 0, 0),
          Join(Add, Cross(Some(CrossRight)),
            joinDeref(megaR2, 0, 2),
            Join(Add, Cross(Some(CrossRight)),
              joinDeref(megaR2, 0, 1),
              joinDeref(megaR2, 0, 0)))))

      megaReduce(input, findReductions(input, evalCtx)) mustEqual expected
    }

    "where a single set is being reduced three times" in {
      val load = dag.AbsoluteLoad(Const(CString("/foo")))

      val red1 = Count
      val r1 = dag.Reduce(red1, load)

      val red3 = StdDev
      val r3 = dag.Reduce(red3, load)

      val input = Join(Add, Cross(Some(CrossRight)), r1, Join(Sub, Cross(Some(CrossRight)), r1, r3))

      val leaf = trans.Leaf(trans.Source)
      val megaR = MegaReduce(List((leaf, List(red1, red3))), load)

      val expected = Join(Add, Cross(Some(CrossRight)),
        joinDeref(megaR, 0, 1),
        Join(Sub, Cross(Some(CrossRight)),
          joinDeref(megaR, 0, 1),
          joinDeref(megaR, 0, 0)))

      megaReduce(input, findReductions(input, evalCtx)) mustEqual expected
    }

    "where three reductions use three different trans specs" in {
      import trans._

      val load = dag.AbsoluteLoad(Const(CString("/hom/heightWeightAcrossSlices")))

      val min = Min
      val max = Max
      val mean = Mean

      val id = Join(DerefObject, Cross(None), load, Const(CString("userId")))
      val height = Join(DerefObject, Cross(None), load, Const(CString("height")))
      val weight = Join(DerefObject, Cross(None), load, Const(CString("weight")))

      val r1 = dag.Reduce(min, id)
      val r2 = dag.Reduce(max, height)
      val r3 = dag.Reduce(mean, weight)

      val input = Join(Add, Cross(None), r1, Join(Add, Cross(None), r2, r3))

      val mega = dag.MegaReduce(
        List(
          (DerefObjectStatic(Leaf(Source), CPathField("userId")), List(r1.red)),
          (DerefObjectStatic(Leaf(Source), CPathField("height")), List(r2.red)),
          (DerefObjectStatic(Leaf(Source), CPathField("weight")), List(r3.red))
        ),
        load
      )

      val expected = Join(Add, Cross(None),
        joinDeref(mega, 2, 0),
        Join(Add, Cross(None),
          joinDeref(mega, 1, 0),
          joinDeref(mega, 0, 0)))

      megaReduce(input, findReductions(input, evalCtx)) mustEqual expected
    }

    "where three reductions use two trans specs" in {
      import trans._

      val load = dag.AbsoluteLoad(Const(CString("/hom/heightWeightAcrossSlices")))

      val min = Min
      val max = Max
      val mean = Mean

      val height = Join(DerefObject, Cross(None), load, Const(CString("height")))
      val weight = Join(DerefObject, Cross(None), load, Const(CString("weight")))

      val r1 = dag.Reduce(min, height)
      val r2 = dag.Reduce(max, height)
      val r3 = dag.Reduce(mean, weight)

      val input = Join(Add, Cross(None), r1, Join(Add, Cross(None), r2, r3))

      val mega = dag.MegaReduce(
        List(
          (DerefObjectStatic(Leaf(Source), CPathField("height")), List(r1.red, r2.red)),
          (DerefObjectStatic(Leaf(Source), CPathField("weight")), List(r3.red))
        ),
        load
      )

      val expected = Join(Add, Cross(None),
        joinDeref(mega, 1, 1),
        Join(Add, Cross(None),
          joinDeref(mega, 1, 0),
          joinDeref(mega, 0, 0)))

      megaReduce(input, findReductions(input, evalCtx)) mustEqual expected
    }

    "where three reductions use one trans spec" in {
      import trans._

      val load = dag.AbsoluteLoad(Const(CString("/hom/heightWeightAcrossSlices")))

      val min = Min
      val max = Max
      val mean = Mean

      val weight = Join(DerefObject, Cross(None), load, Const(CString("weight")))

      val r1 = dag.Reduce(min, weight)
      val r2 = dag.Reduce(max, weight)
      val r3 = dag.Reduce(mean, weight)

      val input = Join(Add, Cross(None), r1, Join(Add, Cross(None), r2, r3))

      val mega = dag.MegaReduce(
        List(
          (DerefObjectStatic(Leaf(Source), CPathField("weight")), List(r1.red, r2.red, r3.red))
        ),
        load
      )

      val expected = Join(Add, Cross(None),
        joinDeref(mega, 0, 2),
        Join(Add, Cross(None),
          joinDeref(mega, 0, 1),
          joinDeref(mega, 0, 0)))

      megaReduce(input, findReductions(input, evalCtx)) mustEqual expected
    }

    "where one reduction uses three trans spec" in {
      import trans._

      val load = dag.AbsoluteLoad(Const(CString("/hom/heightWeightAcrossSlices")))

      val mean = Mean

      val id = Join(DerefObject, Cross(None), load, Const(CString("userId")))
      val height = Join(DerefObject, Cross(None), load, Const(CString("height")))
      val weight = Join(DerefObject, Cross(None), load, Const(CString("weight")))

      val r1 = dag.Reduce(mean, id)
      val r2 = dag.Reduce(mean, height)
      val r3 = dag.Reduce(mean, weight)

      val input = Join(Add, Cross(None), r1, Join(Add, Cross(None), r2, r3))

      val mega = dag.MegaReduce(
        List(
          (DerefObjectStatic(Leaf(Source), CPathField("userId")), List(mean)),
          (DerefObjectStatic(Leaf(Source), CPathField("height")), List(mean)),
          (DerefObjectStatic(Leaf(Source), CPathField("weight")), List(mean))
        ),
        load
      )

      val expected = Join(Add, Cross(None),
        joinDeref(mega, 2, 0),
        Join(Add, Cross(None),
          joinDeref(mega, 1, 0),
          joinDeref(mega, 0, 0)))

      megaReduce(input, findReductions(input, evalCtx)) mustEqual expected
    }

    "in a split" in {
      // nums := dataset(//hom/numbers)
      // sums('n) :=
      //   m := max(nums where nums < 'n)
      //   (nums where nums = 'n) + m     -- actually, we used split root, but close enough
      // sums

      val nums = dag.AbsoluteLoad(Const(CString("/hom/numbers")))

      val reduction = Max

      val id = new Identifier

      val input = dag.Split(
        dag.Group(1, nums, UnfixedSolution(0, nums)),
        Join(Add, Cross(None),
          SplitGroup(1, nums.identities, id),
          dag.Reduce(reduction,
            Filter(IdentitySort,
              nums,
              Join(Lt, Cross(None),
                nums,
                SplitParam(0, id))))), id)

      val parent = Filter(IdentitySort,
        nums,
        Join(Lt, Cross(None),
          nums,
          SplitParam(0, id)))  //TODO need a window function

      val megaR = MegaReduce(List((trans.Leaf(trans.Source), List(reduction))), parent)

      val expected = dag.Split(
        dag.Group(1, nums, UnfixedSolution(0, nums)),
        Join(Add, Cross(None),
          SplitGroup(1, nums.identities, id),
          joinDeref(megaR, 0, 0)), id)

      megaReduce(input, findReductions(input, evalCtx)) mustEqual expected
    }

    "in a split that contains two reductions of the same dataset" in {
      //
      // clicks := dataset(//clicks)
      // histogram := solve 'user
      //   { user: 'user, min: min(clicks.foo where clicks.user = 'user), max: max(clicks.foo where clicks.user = 'user) }
      //
      //  --if max is taken instead of clicks.bar, the change in the DAG not show up inside the Reduce, and so is hard to track the reductions
      // histogram

      val clicks = dag.AbsoluteLoad(Const(CString("/clicks")))

      val id = new Identifier

      val input = dag.Split(
        dag.Group(1,
          Join(DerefObject, Cross(None), clicks, Const(CString("foo"))),
          UnfixedSolution(0,
            Join(DerefObject, Cross(None),
              clicks,
              Const(CString("user"))))),
        Join(JoinObject, Cross(None),
          Join(WrapObject, Cross(None),
            Const(CString("user")),
            SplitParam(0, id)),
          Join(JoinObject, Cross(None),
            Join(WrapObject, Cross(None),
              Const(CString("min")),
              dag.Reduce(Min,
                SplitGroup(1, Identities.Specs(Vector(LoadIds("/clicks"))), id))),
            Join(WrapObject, Cross(None),
              Const(CString("max")),
              dag.Reduce(Max,
                SplitGroup(1, Identities.Specs(Vector(LoadIds("/clicks"))), id))))), id)

      val parent = SplitGroup(1, clicks.identities, id)
      val red1 = dag.Reduce(Min, parent)
      val red2 = dag.Reduce(Max, parent)
      val megaR = MegaReduce(List((trans.Leaf(trans.Source), List(red1.red, red2.red))), parent)

      val expected = dag.Split(
        dag.Group(1,
          Join(DerefObject, Cross(None), clicks, Const(CString("foo"))),
          UnfixedSolution(0,
            Join(DerefObject, Cross(None),
              clicks,
              Const(CString("user"))))),
        Join(JoinObject, Cross(None),
          Join(WrapObject, Cross(None),
            Const(CString("user")),
            SplitParam(0, id)),
          Join(JoinObject, Cross(None),
            Join(WrapObject, Cross(None),
              Const(CString("min")),
              joinDeref(megaR, 0, 1)),
            Join(WrapObject, Cross(None),
              Const(CString("max")),
              joinDeref(megaR, 0, 0)))), id)

      megaReduce(input, findReductions(input, evalCtx)) mustEqual expected
    }
  }

  "reduction finder" should {
    "in a load, find no reductions when there aren't any" in {
      val input = dag.AbsoluteLoad(Const(CString("/foo")))
      val expected = MegaReduceState(
        Map(),
        Map(),
        Map(),
        Map()
      )

      findReductions(input, evalCtx) mustEqual expected
    }

    "in a single reduction" in {
      val load = dag.AbsoluteLoad(Const(CString("/foo")))
      val reduction = Count
      val r = dag.Reduce(reduction, load)

      val expected = MegaReduceState(
        Map(r -> load),
        Map(load -> List(load)),
        Map(load -> List(r)),
        Map(load -> trans.Leaf(trans.Source))
      )

      findReductions(r, evalCtx) mustEqual expected
    }

    "in a join of two reductions on the same dataset #2" in {
      val load = dag.AbsoluteLoad(Const(CString("/foo")))
      val r1 = dag.Reduce(Count, load)
      val r2 = dag.Reduce(StdDev, load)

      val input = Join(Add, Cross(None), r1, r2)

      val expected = MegaReduceState(
        Map(r1 -> load, r2 -> load),
        Map(load -> List(load)),
        Map(load -> List(r1, r2)),
        Map(load -> trans.Leaf(trans.Source))
      )

      findReductions(input, evalCtx) mustEqual expected
    }

    "findReductions given a reduction inside a reduction" in {
      val load = dag.AbsoluteLoad(Const(CString("/foo")))
      val r1 = dag.Reduce(Mean, load)
      val r2 = dag.Reduce(Count, r1)

      val expected = MegaReduceState(
        Map(r2 -> r1, r1 -> load),
        Map(load -> List(load), r1 -> List(r1)),
        Map(load -> List(r1), r1 -> List(r2)),
        Map(load -> trans.Leaf(trans.Source), r1 -> trans.Leaf(trans.Source))
      )

      findReductions(r2, evalCtx) mustEqual expected
    }

    "findReductions given two reductions inside a reduction" in {
      val foo = dag.AbsoluteLoad(Const(CString("/foo")))
      val mean = dag.Reduce(Mean, foo)
      val stdDev = dag.Reduce(StdDev, foo)
      val parentCount = dag.Join(Add, Cross(None), mean, stdDev)

      val input = dag.Reduce(Count, parentCount)

      val expected = MegaReduceState(
        Map(
          mean -> foo,
          stdDev -> foo,
          input -> parentCount
        ),
        Map(
          foo -> List(foo),
          parentCount -> List(parentCount)
        ),
        Map(
          foo -> List(mean, stdDev),
          parentCount -> List(input)
        ),
        Map(
          foo -> trans.Leaf(trans.Source),
          parentCount -> trans.Leaf(trans.Source)
        )
      )

      findReductions(input, evalCtx) mustEqual expected
    }

    // TODO: need to test reductions whose parents are splits
    "findReductions inside a Split" in {
      val clicks = dag.AbsoluteLoad(Const(CString("/clicks")))
      val red = Count
      val count = dag.Reduce(red, clicks)

      val id = new Identifier

      val input = dag.Split(
        dag.Group(1,
          clicks,
          UnfixedSolution(0, count)),
        SplitParam(1, id), id)

      val expected = MegaReduceState(
        Map(count -> clicks),
        Map(clicks -> List(clicks)),
        Map(clicks -> List(count)),
        Map(clicks -> trans.Leaf(trans.Source))
      )

      findReductions(input, evalCtx) mustEqual expected
    }


    "in a join where only one side is a reduction" in {
      val load = dag.AbsoluteLoad(Const(CString("/foo")))

      "right" in {
        val r = dag.Reduce(StdDev, load)
        val input = Join(Add, Cross(None), dag.Operate(Neg, load), r)

        val expected = MegaReduceState(
          Map(r -> load),
          Map(load -> List(load)),
          Map(load -> List(r)),
          Map(load -> trans.Leaf(trans.Source))
        )

        findReductions(input, evalCtx) mustEqual expected
      }
      "left" in {
        val r = dag.Reduce(Count, load)
        val input = Join(Add, Cross(Some(CrossRight)), r, dag.Operate(Neg, load))

        val expected = MegaReduceState(
          Map(r -> load),
          Map(load -> List(load)),
          Map(load -> List(r)),
          Map(load -> trans.Leaf(trans.Source))
        )

        findReductions(input, evalCtx) mustEqual expected
      }
    }

    "in a split" in {
      // nums := dataset(//hom/numbers)
      // sums('n) :=
      //   m := max(nums where nums < 'n)
      //   (nums where nums = 'n) + m     -- actually, we used split root, but close enough
      // sums

      val nums = dag.AbsoluteLoad(Const(CString("/hom/numbers")))

      val id = new Identifier

      lazy val j = Join(Lt, Cross(None), nums, SplitParam(0, id))
      lazy val parent = Filter(IdentitySort, nums, j)

      lazy val splitGroup = SplitGroup(1, nums.identities, id)
      lazy val r = dag.Reduce(Max, parent)

      lazy val group = dag.Group(1, nums, UnfixedSolution(0, nums))
      lazy val join = Join(Add, Cross(None), splitGroup, r)
      lazy val input = dag.Split(group, join, id)

      lazy val expected = MegaReduceState(
        Map(r -> parent),
        Map(parent -> List(parent)),
        Map(parent -> List(r)),
        Map(parent -> trans.Leaf(trans.Source))
      )

      findReductions(input, evalCtx) mustEqual expected
    }

    "in a split that contains two reductions of the same dataset #2" in {
      // clicks := dataset(//clicks)
      // histogram('user) :=
      //   { user: 'user,
      //     min: min(clicks.foo where clicks.user = 'user),
      //     max: max(clicks.foo where clicks.user = 'user) }
      // histogram
      //
      // -- if max is taken instead of clicks.bar, the change in the DAG does
      // -- not show up inside the Reduce, so it's hard to track the reductions

      val clicks = dag.AbsoluteLoad(Const(CString("/clicks")))

      val fooRoot = Const(CString("foo"))
      val userRoot = Const(CString("user"))
      val minRoot = Const(CString("min"))
      val maxRoot = Const(CString("max"))

      val clicksFoo = Join(DerefObject, Cross(None), clicks, fooRoot)
      val clicksUser = Join(DerefObject, Cross(None), clicks, userRoot)
      val group1 = dag.Group(1, clicksFoo, UnfixedSolution(0, clicksUser))

      val id = new Identifier

      lazy val parent = SplitGroup(1, Identities.Specs(Vector(LoadIds("/clicks"))), id)
      lazy val r1 = dag.Reduce(Min, parent)
      lazy val r2 = dag.Reduce(Max, parent)

      lazy val input: dag.Split = dag.Split(
        group1,
        Join(JoinObject, Cross(None),
          Join(WrapObject, Cross(None),
            userRoot,
            SplitParam(0, id)),
          Join(JoinObject, Cross(None),
            Join(WrapObject, Cross(None), minRoot, r1),
            Join(WrapObject, Cross(None), maxRoot, r2))), id)

      val expected = MegaReduceState(
        Map(r1 -> parent, r2 -> parent),
        Map(parent -> List(parent)),
        Map(parent -> List(r1, r2)),
        Map(parent -> trans.Leaf(trans.Source))
      )

      findReductions(input, evalCtx) mustEqual expected
    }
  }

  def joinDeref(left: DepGraph, first: Int, second: Int): DepGraph =
    Join(DerefArray, Cross(Some(CrossLeft)),
      Join(DerefArray, Cross(Some(CrossLeft)),
        left,
        Const(CLong(first))),
      Const(CLong(second)))
}

object ReductionFinderSpecs extends ReductionFinderSpecs

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
import quasar.precog.util.Identifier
import quasar.yggdrasil.bytecode._
import quasar.yggdrasil._
import quasar.precog.TestSupport._

object DAGSpecs extends Specification with DAG with FNDummyModule {
  import instructions._
  import dag._

  type Lib = RandomLibrary
  val library = new RandomLibrary {}
  import library._

  "dag decoration" should {
    "recognize root instructions" in {
      "push_str" >> {
        decorate(Vector(Line(1, 1, ""), PushString("test"))) mustEqual Right(Const(CString("test"))(Line(1, 1, "")))
      }

      "push_num" >> {
        decorate(Vector(Line(1, 1, ""), PushNum("42"))) mustEqual Right(Const(CLong(42))(Line(1, 1, "")))
      }

      "push_true" >> {
        decorate(Vector(Line(1, 1, ""), PushTrue)) mustEqual Right(Const(CTrue)(Line(1, 1, "")))
      }

      "push_false" >> {
        decorate(Vector(Line(1, 1, ""), PushFalse)) mustEqual Right(Const(CFalse)(Line(1, 1, "")))
      }

      "push_null" >> {
        decorate(Vector(Line(1, 1, ""), PushNull)) mustEqual Right(Const(CNull)(Line(1, 1, "")))
      }

      "push_object" >> {
        decorate(Vector(Line(1, 1, ""), PushObject)) mustEqual Right(Const(RObject.empty)(Line(1, 1, "")))
      }

      "push_array" >> {
        decorate(Vector(Line(1, 1, ""), PushArray)) mustEqual Right(Const(RArray.empty)(Line(1, 1, "")))
      }

      "push_undefined" >> {
        decorate(Vector(Line(1, 1, ""), PushUndefined)) mustEqual Right(Undefined(Line(1, 1, "")))
      }
    }

    "recognize a new instruction" in {
      decorate(Vector(Line(1, 1, ""), PushNum("5"), Map1(instructions.New))) mustEqual Right(dag.New(Const(CLong(5))(Line(1, 1, "")))(Line(1, 1, "")))
    }

    "parse out absolute_load" in {
      val result = decorate(Vector(Line(1, 1, ""), PushString("/foo"), instructions.AbsoluteLoad))
      result mustEqual Right(dag.AbsoluteLoad(Const(CString("/foo"))(Line(1, 1, "")))(Line(1, 1, "")))
    }

    "parse out relative_load" in {
      val result = decorate(Vector(Line(1, 1, ""), PushString("/foo"), instructions.RelativeLoad))
      result mustEqual Right(dag.RelativeLoad(Const(CString("/foo"))(Line(1, 1, "")))(Line(1, 1, "")))
    }

    "parse out map1" in {
      val result = decorate(Vector(Line(1, 1, ""), PushTrue, Map1(Neg)))
      result mustEqual Right(Operate(Neg, Const(CTrue)(Line(1, 1, "")))(Line(1, 1, "")))
    }

    "parse out reduce" in {
      val result = decorate(Vector(Line(1, 1, ""), PushFalse, instructions.Reduce(BuiltInReduction(Reduction(Vector(), "count", 0x2000)))))
      result mustEqual Right(dag.Reduce(Reduction(Vector(), "count", 0x2000), Const(CFalse)(Line(1, 1, "")))(Line(1, 1, "")))
    }

    "parse out distinct" in {
      val result = decorate(Vector(Line(1, 1, ""), PushNull, instructions.Distinct))
      result mustEqual Right(dag.Distinct(Const(CNull)(Line(1, 1, "")))(Line(1, 1, "")))
    }

    // TODO morphisms

    "parse an array join" in {
      val result = decorate(Vector(
        Line(1, 1, ""),
        PushString("/summer_games/london_medals"),
        instructions.AbsoluteLoad,
        Dup,
        PushString("Weight"),
        Map2Cross(DerefObject),
        instructions.Reduce(BuiltInReduction(Reduction(Vector(), "max", 0x2001))),
        Map1(WrapArray),
        Swap(1),
        PushString("HeightIncm"),
        Map2Cross(DerefObject),
        instructions.Reduce(BuiltInReduction(Reduction(Vector(), "max", 0x2001))),
        Map1(WrapArray),
        Map2Cross(JoinArray)))

      val line = Line(1, 1, "")
      val medals = dag.AbsoluteLoad(Const(CString("/summer_games/london_medals"))(line))(line)

      val expected = Join(JoinArray, Cross(None),
        Operate(WrapArray,
          dag.Reduce(Reduction(Vector(), "max", 0x2001),
            Join(DerefObject, Cross(None),
              medals,
              Const(CString("Weight"))(line))(line))(line))(line),
        Operate(WrapArray,
          dag.Reduce(Reduction(Vector(), "max", 0x2001),
            Join(DerefObject, Cross(None),
              medals,
              Const(CString("HeightIncm"))(line))(line))(line))(line))(line)


      result mustEqual Right(expected)
    }

    "parse a single-level split" in {
      val line = Line(1, 1, "")

      val result = decorate(Vector(
        line,
        PushTrue,
        Dup,
        KeyPart(1),
        Swap(1),
        instructions.Group(2),
        instructions.Split,
        PushGroup(2),
        PushKey(1),
        IUnion,
        Merge))

      result must beLike {
        case Right(
          s @ dag.Split(
            dag.Group(2,
              Const(CTrue),
              UnfixedSolution(1, Const(CTrue))),
            IUI(true,
              sg: SplitGroup,
              sp: SplitParam), id)) => {

          sp.id mustEqual 1
          sg.id mustEqual 2
          sg.identities mustEqual Identities.Specs(Vector())

          sp.parentId mustEqual id
          sg.parentId mustEqual id
        }
      }
    }

    "parse a bi-level split" in {
      val line = Line(1, 1, "")

      val result = decorate(Vector(
        line,
        PushTrue,
        KeyPart(1),
        PushFalse,
        instructions.Group(2),
        instructions.Split,
        PushGroup(2),
        KeyPart(3),
        PushKey(1),
        instructions.Group(4),
        instructions.Split,
        PushGroup(4),
        PushKey(3),
        IUnion,
        Merge,
        Merge))

      result must beLike {
        case Right(
          s1 @ dag.Split(
            dag.Group(2, Const(CFalse), UnfixedSolution(1, Const(CTrue))),
            s2 @ dag.Split(
              dag.Group(4, sp1: SplitParam, UnfixedSolution(3, sg1: SplitGroup)),
              IUI(true,
                sg2: SplitGroup,
                sp2: SplitParam), id2), id1)) => {

          sp1.id mustEqual 1
          sg1.id mustEqual 2
          sg1.identities mustEqual Identities.Specs(Vector())

          sp1.parentId mustEqual id1
          sg1.parentId mustEqual id1

          sp2.id mustEqual 3
          sp2.identities mustEqual Identities.Specs(Vector())

          sp2.parentId mustEqual id2
          sg2.parentId mustEqual id2
        }
      }
    }

    "parse a bi-level split with intermediate usage" in {
      val line = Line(1, 1, "")

      val result = decorate(Vector(
        line,
        PushTrue,
        KeyPart(1),
        PushFalse,
        instructions.Group(2),
        instructions.Split,
        PushGroup(2),
        PushKey(1),
        Map2Cross(Add),
        PushNum("42"),
        KeyPart(3),
        PushFalse,
        instructions.Group(4),
        instructions.Split,
        PushGroup(4),
        IUnion,
        Merge,
        Merge))

      result must beLike {
        case Right(
          s1 @ dag.Split(
            dag.Group(2, Const(CFalse), UnfixedSolution(1, Const(CTrue))),
            s2 @ dag.Split(
              dag.Group(4, Const(CFalse), UnfixedSolution(3, Const(CLong(42)))),
              IUI(true,
                Join(Add, Cross(_),
                  sg1: SplitGroup,
                  sp1: SplitParam),
                sg2: SplitGroup), id2), id1)) => {

          sp1.id mustEqual 1
          sg1.id mustEqual 2
          sg1.identities mustEqual Identities.Specs(Vector())

          sg2.id mustEqual 4
          sg2.identities mustEqual Identities.Specs(Vector())

          sp1.parentId mustEqual id1
          sg1.parentId mustEqual id1

          sg2.parentId mustEqual id2
        }
      }
    }

    "parse a split with merged buckets" >> {
      "union" >> {
        val line = Line(1, 1, "")

        val result = decorate(Vector(
          line,
          PushNum("1"),
          KeyPart(1),
          PushNum("3"),
          KeyPart(1),
          MergeBuckets(false),
          PushNum("2"),
          instructions.Group(3),
          instructions.Split,
          PushGroup(3),
          PushKey(1),
          IUnion,
          Merge))

        result must beLike {
          case Right(
            s @ dag.Split(
              dag.Group(3,
                Const(CLong(2)),
                UnionBucketSpec(
                  UnfixedSolution(1, Const(CLong(1))),
                  UnfixedSolution(1, Const(CLong(3))))),
              IUI(true,
                sg: SplitGroup,
                sp: SplitParam), id)) => {

            sp.id mustEqual 1
            sg.id mustEqual 3
            sg.identities mustEqual Identities.Specs(Vector())

            sg.parentId mustEqual id
            sp.parentId mustEqual id
          }
        }
      }

      "intersect" >> {
        val line = Line(1, 1, "")

        val result = decorate(Vector(
          line,
          PushNum("1"),
          KeyPart(1),
          PushNum("3"),
          KeyPart(1),
          MergeBuckets(true),
          PushNum("2"),
          instructions.Group(3),
          instructions.Split,
          PushGroup(3),
          PushKey(1),
          IUnion,
          Merge))

        result must beLike {
          case Right(
            s @ dag.Split(
              dag.Group(3,
                Const(CLong(2)),
                IntersectBucketSpec(
                  UnfixedSolution(1, Const(CLong(1))),
                  UnfixedSolution(1, Const(CLong(3))))),
              IUI(true,
                sg: SplitGroup,
                sp: SplitParam), id)) => {

            sp.id mustEqual 1
            sg.id mustEqual 3
            sg.identities mustEqual Identities.Specs(Vector())

            sg.parentId mustEqual id
            sp.parentId mustEqual id
          }
        }
      }
    }

    // TODO union zip and zip with multiple keys
    "parse a split with zipped buckets" in {
      val line = Line(1, 1, "")

      val result = decorate(Vector(
        line,
        PushNum("1"),
        KeyPart(1),
        PushNum("2"),
        instructions.Group(2),
        PushNum("3"),
        KeyPart(1),
        PushNum("4"),
        instructions.Group(3),
        MergeBuckets(true),
        instructions.Split,
        PushGroup(2),
        PushGroup(3),
        PushKey(1),
        IUnion,
        IUnion,
        Merge))

      result must beLike {
        case Right(
          s @ dag.Split(
            IntersectBucketSpec(
              dag.Group(2, Const(CLong(2)), UnfixedSolution(1, Const(CLong(1)))),
              dag.Group(3, Const(CLong(4)), UnfixedSolution(1, Const(CLong(3))))),
            IUI(true,
              sg2: SplitGroup,
              IUI(true,
                sg1: SplitGroup,
                sp1: SplitParam)), id)) => {

          sp1.id mustEqual 1
          sg1.id mustEqual 3
          sg1.identities mustEqual Identities.Specs(Vector())

          sg2.id mustEqual 2
          sg2.identities mustEqual Identities.Specs(Vector())

          sg1.parentId mustEqual id
          sp1.parentId mustEqual id
          sg2.parentId mustEqual id
        }
      }
    }

    "accept split which reduces the stack" in {
      val line = Line(1, 1, "")

      val result = decorate(Vector(
        line,
        PushNum("42"),
        PushTrue,
        KeyPart(1),
        PushNull,
        instructions.Group(2),
        instructions.Split,
        PushGroup(2),
        Map2Match(Add),
        Merge))

      result must beLike {
        case Right(
          s @ dag.Split(
            dag.Group(2, Const(CNull), UnfixedSolution(1, Const(CTrue))),
            Join(Add, IdentitySort,
              Const(CLong(42)),
              sg: SplitGroup), id)) => {

          sg.id mustEqual 2
          sg.identities mustEqual Identities.Specs(Vector())
          sg.parentId mustEqual id
        }
      }
    }

    "determine a histogram of a composite key of revenue and campaign" in {
      val line = Line(1, 1, "")

      val result @ Right(dag.Split(_, _, id)) = decorate(Vector(
        Line(1, 1, ""),
        PushString("/organizations"),
        instructions.AbsoluteLoad,
        Dup,
        Dup,
        Dup,
        PushString("revenue"),
        Map2Cross(DerefObject),
        KeyPart(1),
        Swap(1),
        PushString("revenue"),
        Map2Cross(DerefObject),
        instructions.Group(0),
        Swap(1),
        PushString("campaign"),
        Map2Cross(DerefObject),
        KeyPart(3),
        Swap(1),
        Swap(2),
        PushString("campaign"),
        Map2Cross(DerefObject),
        instructions.Group(2),
        MergeBuckets(true),
        PushString("/campaigns"),
        instructions.AbsoluteLoad,
        Dup,
        Swap(2),
        Swap(1),
        PushString("campaign"),
        Map2Cross(DerefObject),
        KeyPart(3),
        Swap(1),
        Swap(2),
        instructions.Group(4),
        MergeBuckets(true),
        instructions.Split,
        PushString("revenue"),
        PushKey(1),
        Map2Cross(WrapObject),
        PushString("num"),
        PushGroup(4),
        instructions.Reduce(BuiltInReduction(Reduction(Vector(), "count", 0x002000))),
        Map2Cross(WrapObject),
        Map2Cross(JoinObject),
        Merge))

      val expectedSpec = IntersectBucketSpec(
        IntersectBucketSpec(
          dag.Group(0,
            Join(DerefObject,Cross(None),
              dag.AbsoluteLoad(Const(CString("/organizations"))(line), JType.JUniverseT)(line),
              Const(CString("revenue"))(line))(line),
            UnfixedSolution(1,
              Join(DerefObject,Cross(None),
                dag.AbsoluteLoad(Const(CString("/organizations"))(line), JType.JUniverseT)(line),
                Const(CString("revenue"))(line))(line))),
          dag.Group(2,
            Join(DerefObject,Cross(None),
              dag.AbsoluteLoad(Const(CString("/organizations"))(line), JType.JUniverseT)(line),
              Const(CString("campaign"))(line))(line),
            UnfixedSolution(3,
              Join(DerefObject,Cross(None),
              dag.AbsoluteLoad(Const(CString("/organizations"))(line), JType.JUniverseT)(line),
              Const(CString("campaign"))(line))(line)))),
        dag.Group(4,
          dag.AbsoluteLoad(Const(CString("/campaigns"))(line), JType.JUniverseT)(line),
          UnfixedSolution(3,
            Join(DerefObject,Cross(None),
              dag.AbsoluteLoad(Const(CString("/campaigns"))(line), JType.JUniverseT)(line),
              Const(CString("campaign"))(line))(line))))

      val expectedTarget = Join(JoinObject,Cross(None),
        Join(WrapObject,Cross(None),
          Const(CString("revenue"))(line),
          SplitParam(1, id)(line))(line),
        Join(WrapObject,Cross(None),
          Const(CString("num"))(line),
          dag.Reduce(Reduction(Vector(), "count", 0x002000),
            SplitGroup(4, Identities.Specs(Vector(LoadIds("/campaigns"))), id)(line))(line))(line))(line)

      val expectedSplit = dag.Split(expectedSpec, expectedTarget, id)(line)

      result mustEqual Right(expectedSplit)
    }

    "recognize a join instruction" in {
      "map2_match" >> {
        val line = Line(1, 1, "")
        val result = decorate(Vector(line, PushTrue, PushFalse, Map2Match(Add)))
        result mustEqual Right(Join(Add, IdentitySort, Const(CTrue)(line), Const(CFalse)(line))(line))
      }

      "map2_cross" >> {
        val line = Line(1, 1, "")
        val result = decorate(Vector(line, PushTrue, PushFalse, Map2Cross(Add)))
        result mustEqual Right(Join(Add, Cross(None), Const(CTrue)(line), Const(CFalse)(line))(line))
      }

      "assert" >> {
        val line = Line(1, 1, "")
        val result = decorate(Vector(line, PushTrue, PushNum("42"), instructions.Assert))
        result mustEqual Right(dag.Assert(Const(CTrue)(line), Const(CLong(42))(line))(line))
      }

      "iunion" >> {
        val line = Line(1, 1, "")
        val result = decorate(Vector(line, PushTrue, PushFalse, IUnion))
        result mustEqual Right(IUI(true, Const(CTrue)(line), Const(CFalse)(line))(line))
      }

      "iintersect" >> {
        val line = Line(1, 1, "")
        val result = decorate(Vector(line, PushTrue, PushFalse, IIntersect))
        result mustEqual Right(IUI(false, Const(CTrue)(line), Const(CFalse)(line))(line))
      }

      "set difference" >> {
        val line = Line(1, 1, "")
        val result = decorate(Vector(line, PushTrue, PushFalse, SetDifference))
        result mustEqual Right(Diff(Const(CTrue)(line), Const(CFalse)(line))(line))
      }
    }

    "parse a filter with null predicate" in {
      val line = Line(1, 1, "")
      val result = decorate(Vector(line, PushFalse, PushTrue, FilterMatch))
      result mustEqual Right(Filter(IdentitySort, Const(CFalse)(line), Const(CTrue)(line))(line))
    }

    "parse a filter_cross" in {
      val line = Line(1, 1, "")
      val result = decorate(Vector(line, PushTrue, PushFalse, FilterCross))
      result mustEqual Right(Filter(Cross(None), Const(CTrue)(line), Const(CFalse)(line))(line))
    }

    "continue processing beyond a filter" in {
      val line = Line(1, 1, "")
      val result = decorate(Vector(line, PushFalse, PushTrue, FilterMatch, Map1(Neg)))
      result mustEqual Right(
        Operate(Neg,
          Filter(IdentitySort,
            Const(CFalse)(line),
            Const(CTrue)(line))(line))(line))
    }

    "parse and factor a dup" in {
      {
        val line = Line(1, 1, "")
        val result = decorate(Vector(line, PushTrue, Dup, IUnion))
        result mustEqual Right(IUI(true, Const(CTrue)(line), Const(CTrue)(line))(line))
      }

      {
        val line = Line(1, 1, "")
        val result = decorate(Vector(line, PushNum("42"), Map1(Neg), Dup, IUnion))
        result mustEqual Right(IUI(true, Operate(Neg, Const(CLong(42))(line))(line), Operate(Neg, Const(CLong(42))(line))(line))(line))
      }
    }

    "parse and factor a swap" in {
      "1" >> {
        val line = Line(1, 1, "")
        val result = decorate(Vector(line, PushFalse, PushTrue, Swap(1), IUnion))
        result mustEqual Right(IUI(true, Const(CTrue)(line), Const(CFalse)(line))(line))
      }

      "3" >> {
        val line = Line(1, 1, "")
        val result = decorate(Vector(line, PushTrue, PushString("foo"), PushFalse, PushNum("42"), Swap(3), IUnion, IUnion, IUnion))
        result mustEqual Right(
          IUI(true,
            Const(CLong(42))(line),
            IUI(true,
              Const(CString("foo"))(line),
              IUI(true, Const(CFalse)(line), Const(CTrue)(line))(line))(line))(line))
      }
    }

    // TODO line info
  }

  "stream validation" should {
    "reject the empty stream" in {
      decorate(Vector()) mustEqual Left(EmptyStream)
    }

    "reject a line-less stream" in {
      decorate(Vector(PushTrue)) mustEqual Left(UnknownLine)
      decorate(Vector(PushTrue, PushFalse, Map1(Comp))) mustEqual Left(UnknownLine)
    }

    "detect stack underflow" in {
      "map1" >> {     // historic sidebar: since we don't have pop, this is the *only* map1 underflow case!
        val instr = Map1(Neg)
        decorate(Vector(Line(1, 1, ""), instr)) mustEqual Left(StackUnderflow(instr))
      }

      "map2_match" >> {
        {
          val instr = Map2Match(Add)
          decorate(Vector(Line(1, 1, ""), instr)) mustEqual Left(StackUnderflow(instr))
        }

        {
          val instr = Map2Match(Add)
          decorate(Vector(Line(1, 1, ""), PushTrue, instr)) mustEqual Left(StackUnderflow(instr))
        }

        {
          val instr = Map2Match(Add)
          decorate(Vector(Line(1, 1, ""), PushTrue, Map1(Comp), instr, Map2Match(Sub))) mustEqual Left(StackUnderflow(instr))
        }
      }

      "map2_cross" >> {
        {
          val instr = Map2Cross(Add)
          decorate(Vector(Line(1, 1, ""), instr)) mustEqual Left(StackUnderflow(instr))
        }

        {
          val instr = Map2Cross(Add)
          decorate(Vector(Line(1, 1, ""), PushTrue, instr)) mustEqual Left(StackUnderflow(instr))
        }

        {
          val instr = Map2Cross(Add)
          decorate(Vector(Line(1, 1, ""), PushTrue, Map1(Comp), instr, Map2Cross(Sub))) mustEqual Left(StackUnderflow(instr))
        }
      }

      "reduce" >> {     // similar to map1, only one underflow case!
        val instr = instructions.Reduce(BuiltInReduction(Reduction(Vector(), "count", 0x2000)))
        decorate(Vector(Line(1, 1, ""), instr)) mustEqual Left(StackUnderflow(instr))
      }

      "set-reduce" >> {     // similar to map1, only one underflow case!
        val instr = instructions.Distinct
        decorate(Vector(Line(1, 1, ""), instr)) mustEqual Left(StackUnderflow(instr))
      }

      "iunion" >> {     // similar to map1, only one underflow case!
        val instr = IUnion
        decorate(Vector(Line(1, 1, ""), instr)) mustEqual Left(StackUnderflow(instr))
      }

      "iintersect" >> {     // similar to map1, only one underflow case!
        val instr = IIntersect
        decorate(Vector(Line(1, 1, ""), instr)) mustEqual Left(StackUnderflow(instr))
      }

      "set difference" >> {     // similar to map1, only one underflow case!
        val instr = SetDifference
        decorate(Vector(Line(1, 1, ""), instr)) mustEqual Left(StackUnderflow(instr))
      }

      "split" >> {     // similar to map1, only one underflow case!
        val instr = instructions.Split
        decorate(Vector(Line(1, 1, ""), instr)) mustEqual Left(StackUnderflow(instr))
      }

      // merge cannot stack underflow; curious, no?

      "filter_match" >> {
        {
          val instr = FilterMatch
          decorate(Vector(Line(1, 1, ""), instr)) mustEqual Left(StackUnderflow(instr))
        }

        {
          val instr = FilterMatch
          decorate(Vector(Line(1, 1, ""), PushTrue, instr)) mustEqual Left(StackUnderflow(instr))
        }

        {
          val instr = FilterMatch
          decorate(Vector(Line(1, 1, ""), PushTrue, PushTrue, Map2Match(Add), instr)) mustEqual Left(StackUnderflow(instr))
        }
      }

      "filter_cross" >> {
        {
          val instr = FilterCross
          decorate(Vector(Line(1, 1, ""), instr)) mustEqual Left(StackUnderflow(instr))
        }

        {
          val instr = FilterCross
          decorate(Vector(Line(1, 1, ""), PushTrue, instr)) mustEqual Left(StackUnderflow(instr))
        }

        {
          val instr = FilterCross
          decorate(Vector(Line(1, 1, ""), PushTrue, PushTrue, Map2Match(Add), instr)) mustEqual Left(StackUnderflow(instr))
        }
      }

      "dup" >> {
        decorate(Vector(Line(1, 1, ""), Dup)) mustEqual Left(StackUnderflow(Dup))
      }

      "swap" >> {
        {
          val instr = Swap(1)
          decorate(Vector(Line(1, 1, ""), instr)) mustEqual Left(StackUnderflow(instr))
        }

        {
          val instr = Swap(1)
          decorate(Vector(Line(1, 1, ""), PushTrue, instr)) mustEqual Left(StackUnderflow(instr))
        }

        {
          val instr = Swap(2)
          decorate(Vector(Line(1, 1, ""), PushTrue, PushTrue, instr)) mustEqual Left(StackUnderflow(instr))
        }

        {
          val instr = Swap(5)
          decorate(Vector(Line(1, 1, ""), PushTrue, PushTrue, instr)) mustEqual Left(StackUnderflow(instr))
        }
      }

      "load_local" >> {
        val instr = instructions.AbsoluteLoad
        decorate(Vector(Line(1, 1, ""), instr)) mustEqual Left(StackUnderflow(instr))
      }
    }

    "reject multiple stack values at end" in {
      decorate(Vector(Line(1, 1, ""), PushTrue, PushFalse)) mustEqual Left(MultipleStackValuesAtEnd)
      decorate(Vector(Line(1, 1, ""), PushTrue, PushFalse, PushNum("42"))) mustEqual Left(MultipleStackValuesAtEnd)
      decorate(Vector(Line(1, 1, ""), PushTrue, PushFalse, PushNum("42"), PushString("foo"))) mustEqual Left(MultipleStackValuesAtEnd)
    }

    "reject negative swap depth" in {
      {
        val instr = Swap(-1)
        decorate(Vector(Line(1, 1, ""), PushTrue, instr)) mustEqual Left(NonPositiveSwapDepth(instr))
      }

      {
        val instr = Swap(-255)
        decorate(Vector(Line(1, 1, ""), PushTrue, instr)) mustEqual Left(NonPositiveSwapDepth(instr))
      }
    }

    "reject zero swap depth" in {
      val instr = Swap(0)
      decorate(Vector(Line(1, 1, ""), PushTrue, instr)) mustEqual Left(NonPositiveSwapDepth(instr))
    }

    "reject merge with deepened stack" in {
      decorate(Vector(
        Line(1, 1, ""),
        PushTrue,
        KeyPart(1),
        PushFalse,
        instructions.Group(2),
        instructions.Split,
        PushKey(1),
        PushGroup(2),
        PushFalse,
        Merge,
        Drop,
        Drop)) mustEqual Left(MergeWithUnmatchedTails)
    }

    "accept merge with reduced (but reordered) stack" in {
      val line = Line(1, 1, "")

      val result @ Right(IUI(_, _, dag.Split(_, _, id))) = decorate(Vector(
        line,
        PushTrue,
        PushFalse,
        PushNum("42"),
        KeyPart(1),
        PushNum("12"),
        instructions.Group(2),
        instructions.Split,
        PushGroup(2),
        Swap(1),
        Swap(2),
        IUnion,
        Merge,
        IIntersect))

      val split = dag.Split(
        dag.Group(2, Const(CLong(12))(line), UnfixedSolution(1, Const(CLong(42))(line))),
        IUI(true,
          SplitGroup(2, Identities.Specs(Vector()), id)(line),
          Const(CTrue)(line))(line), id)(line)

      val expect = IUI(false, Const(CFalse)(line), split)(line)

      result mustEqual Right(expect)
    }

    "reject unmatched merge" in {
      decorate(Vector(Line(1, 1, ""), PushTrue, Merge)) mustEqual Left(UnmatchedMerge)
    }

    "reject split without corresponding merge" in {
      decorate(Vector(Line(1, 1, ""),
        PushTrue,
        KeyPart(1),
        PushFalse,
        instructions.Group(2),
        instructions.Split)) mustEqual Left(UnmatchedSplit)
    }

    "reject split which increases the stack" in {
      val line = Line(1, 1, "")

      val result = decorate(Vector(
        line,
        PushTrue,
        KeyPart(1),
        PushFalse,
        instructions.Group(2),
        instructions.Split,
        PushGroup(2),
        PushTrue,
        Merge))

      result mustEqual Left(MergeWithUnmatchedTails)
    }
  }

  "mapDown" should {
    "rewrite a AbsoluteLoad shared across Split branches to the same object" in {
      val line = Line(1, 1, "")
      val load = dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line)

      val id = new Identifier

      val input = dag.Split(
        dag.Group(1, load, UnfixedSolution(0, load)),
        SplitParam(0, id)(line), id)(line)

      val result = input.mapDown { recurse => {
        case graph @ dag.AbsoluteLoad(Const(CString(path)), tpe) =>
          dag.AbsoluteLoad(Const(CString("/foo" + path))(graph.loc), tpe)(graph.loc)
      }}

      result must beLike {
        case dag.Split(dag.Group(_, load1, UnfixedSolution(_, load2)), _, _) =>
          load1 must be(load2)
      }
    }
  }

  "foldDown" should {
    "look within a Split branch" in {
      val line = Line(1, 1, "")
      val load = dag.AbsoluteLoad(Const(CString("/clicks"))(line))(line)

      val id = new Identifier

      val input = dag.Split(
        dag.Group(1, load, UnfixedSolution(0, load)),
        SplitParam(0, id)(line), id)(line)

      import scalaz.std.anyVal._
      val result = input.foldDown[Int](true) {
        case _: AbsoluteLoad => 1
      }

      result mustEqual 2
    }
  }
}

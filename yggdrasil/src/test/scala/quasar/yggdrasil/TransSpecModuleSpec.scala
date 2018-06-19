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

package quasar.yggdrasil

import org.specs2.execute.Result
import quasar.ScalazSpecs2Instances
import quasar.precog.common._
import quasar.precog.TestSupport._
import quasar.yggdrasil.bytecode.JNumberT
import quasar.yggdrasil.table.{CF1, CF2, CFN}

trait TransSpecModuleSpec extends TransSpecModule with FNDummyModule with SpecificationLike with ScalazSpecs2Instances {
  import trans._
  import CPath._

  "concatChildren" should {
    "transform a CPathTree into a TransSpec" in {
      val tree: CPathTree[Int] = RootNode(Seq(
        FieldNode(CPathField("bar"),
          Seq(
            IndexNode(CPathIndex(0), Seq(LeafNode(4))),
            IndexNode(CPathIndex(1), Seq(FieldNode(CPathField("baz"), Seq(LeafNode(6))))),
            IndexNode(CPathIndex(2), Seq(LeafNode(2))))),
        FieldNode(CPathField("foo"), Seq(LeafNode(0)))))

      val result = TransSpec.concatChildren(tree)

      def leafSpec(idx: Int) = DerefArrayStatic(Leaf(Source), CPathIndex(idx))

      val expected = InnerObjectConcat(
        WrapObject(
          InnerArrayConcat(
            InnerArrayConcat(
              WrapArray(leafSpec(4)),
              WrapArray(WrapObject(leafSpec(6),"baz"))),
            WrapArray(DerefArrayStatic(Leaf(Source),CPathIndex(2)))),"bar"),
          WrapObject(
            leafSpec(0),"foo"))

      result mustEqual expected
    }
  }

  "rephrase" should {
    import scalaz.syntax.std.option._
    def rephraseTest[A <: SourceType](projection: TransSpec[A], rootSource: A, root: TransSpec1, expected: Option[TransSpec1]): Result = {
      TransSpec.rephrase(projection, rootSource, root) mustEqual expected
    }

    "invert a single-key prefix" in rephraseTest(
      // rephrase(.a, .a.b) --> .b
      projection = DerefObjectStatic(
        Leaf(Source),
        CPathField("a")),
      root = DerefObjectStatic(DerefObjectStatic(
        Leaf(Source),
        CPathField("a")), CPathField("b")),
      rootSource = Source,
      expected = DerefObjectStatic(Leaf(Source), CPathField("b")).some
    )

    "invert a multiple-key prefix" in rephraseTest(
      // rephrase(.a.b, .a.b.c) --> .c
      projection = DerefObjectStatic(DerefObjectStatic(
        Leaf(Source), CPathField("a")), CPathField("b")),
      root = DerefObjectStatic(DerefObjectStatic(DerefObjectStatic(
        Leaf(Source), CPathField("a")), CPathField("b")), CPathField("c")),
      rootSource = Source,
      expected = DerefObjectStatic(Leaf(Source), CPathField("c")).some
    )

    "strip a single-index prefix" in rephraseTest(
      // rephrase(.[0], .[0].[1]) --> .[1]
      projection = DerefArrayStatic(
        Leaf(Source), CPathIndex(0)),
      root = DerefArrayStatic(DerefArrayStatic(
        Leaf(Source), CPathIndex(0)), CPathIndex(1)),
      rootSource = Source,
      expected = DerefArrayStatic(Leaf(Source), CPathIndex(1)).some
    )

    "strip a multiple-index prefix" in rephraseTest(
      // rephrase(.[0].[1], .[0].[1].[2]) --> .[2]
      projection = DerefArrayStatic(DerefArrayStatic(Leaf(Source), CPathIndex(0)), CPathIndex(1)),
      root = DerefArrayStatic(DerefArrayStatic(DerefArrayStatic(
        Leaf(Source), CPathIndex(0)), CPathIndex(1)), CPathIndex(2)),
      rootSource = Source,
      expected = DerefArrayStatic(Leaf(Source), CPathIndex(2)).some
    )

    "strip WrapObject" in rephraseTest(
      // rephrase({key: .}, .) --> .key
      projection = WrapObject(Leaf(Source), "key"),
      root = Leaf(Source),
      rootSource = Source,
      expected = DerefObjectStatic(Leaf(Source), CPathField("key")).some
    )

    "strip WrapArray" in rephraseTest(
      // rephrase([.], .) --> .[0]
      projection = WrapArray(Leaf(Source)),
      root = Leaf(Source),
      rootSource = Source,
      expected = DerefArrayStatic(Leaf(Source), CPathIndex(0)).some
    )

    "strip ArraySwap" in rephraseTest(
      // rephrase(swap(., x), .) --> swap(., x)
      projection = ArraySwap(Leaf(Source), 1),
      root = Leaf(Source),
      rootSource = Source,
      expected = ArraySwap(Leaf(Source), 1).some
    )

    "strip OuterArrayConcat" in rephraseTest(
      // rephrase([.] ++ [], .) --> .[0]
      projection = OuterArrayConcat(WrapArray(Leaf(Source))),
      root = Leaf(Source),
      rootSource = Source,
      expected = DerefArrayStatic(Leaf(Source), CPathIndex(0)).some
    )

    "strip OuterArrayConcat with two args" in rephraseTest(
      // rephrase([.], .) -> .[0]
      projection = OuterArrayConcat(WrapArray(Leaf(SourceLeft)), WrapArray(Leaf(SourceRight))),
      root = Leaf(Source),
      rootSource = SourceRight,
      expected = DerefArrayStatic(Leaf(Source), CPathIndex(1)).some
    )

    "strip OuterArrayConcat with nested InnerArrayConcat" in rephraseTest(
      // rephrase(([._1] ++ [._1]) ++ [._2], _2, .) -> .[2]
      projection = OuterArrayConcat(InnerArrayConcat(WrapArray(Leaf(SourceLeft)), WrapArray(Leaf(SourceLeft))), WrapArray(Leaf(SourceRight))),
      root = Leaf(Source),
      rootSource = SourceRight,
      expected = DerefArrayStatic(Leaf(Source), CPathIndex(2)).some
    )

    "strip OuterObjectConcat with two args with the same key, with the second returning none" in rephraseTest(
      // rephrase({k: ._1} ++ {k: ._2}, _1, .) -> none
      projection = OuterObjectConcat(WrapObject(Leaf(SourceLeft), "k"), WrapObject(Leaf(SourceRight), "k")),
      root = Leaf(Source),
      rootSource = SourceLeft,
      expected = None
    )

    "strip OuterObjectConcat with two args with the same key, with the first returning none" in rephraseTest(
      // rephrase({k: ._1} ++ {k: ._2}, _2, .) -> .k
      projection = OuterObjectConcat(WrapObject(Leaf(SourceLeft), "k"), WrapObject(Leaf(SourceRight), "k")),
      root = Leaf(Source),
      rootSource = SourceRight,
      expected = DerefObjectStatic(Leaf(Source), CPathField("k")).some
    )

    "strip OuterObjectConcat with two args with different keys chooses the last" in rephraseTest(
      // rephrase({k1: .} ++ {k2: .}, .) -> .k2
      projection = OuterObjectConcat(WrapObject(Leaf(Source), "k1"), WrapObject(Leaf(Source), "k2")),
      root = Leaf(Source),
      rootSource = Source,
      expected = DerefObjectStatic(Leaf(Source), CPathField("k2")).some
    )

    "strip OuterObjectConcat nested with InnerObjectConcat with two args with different keys chooses the last" in rephraseTest(
      // rephrase({k1: .} ++ {k2: .}, .) --> .k2
      projection = OuterObjectConcat(WrapObject(Leaf(Source), "k1"), InnerObjectConcat(WrapObject(Leaf(Source), "k2"))),
      root = Leaf(Source),
      rootSource = Source,
      expected = DerefObjectStatic(Leaf(Source), CPathField("k2")).some
    )

    "return none when the projection doesn't mention rootSource" in rephraseTest(
      // rephrase(._1, _2, .) --> none
      projection = Leaf(SourceLeft),
      root = Leaf(Source),
      rootSource = SourceRight,
      expected = None
    )

    "strip prefixes with the same target as root" in rephraseTest(
      // rephrase(._1.a, .a.b) --> .b
      projection = DerefObjectStatic(Leaf(SourceLeft), CPathField("a")),
      root = DerefObjectStatic(DerefObjectStatic(
        Leaf(Source), CPathField("a")), CPathField("b")),
      rootSource = SourceLeft,
      expected = DerefObjectStatic(Leaf(Source), CPathField("b")).some
    )

    "deal with single OuterArrayConcat" in rephraseTest(
      // rephrase([.key] ++ [1], .key) --> .[0]
      projection = OuterArrayConcat(
        WrapArray(DerefObjectStatic(Leaf(Source), CPathField("key"))),
        WrapArray(ConstLiteral(CLong(1), Leaf(Source)))
      ),
      root = DerefObjectStatic(Leaf(Source), CPathField("key")),
      rootSource = Source,
      expected = DerefArrayStatic(Leaf(Source), CPathIndex(0)).some
    )

    "deal with nested OuterArrayConcat" in rephraseTest(
      // rephrase([.key] ++ [1], .key) --> .[0]
      projection = OuterArrayConcat(
        WrapArray(DerefObjectStatic(Leaf(Source), CPathField("key"))),
        OuterArrayConcat(WrapArray(ConstLiteral(CLong(1), Leaf(Source))))
      ),
      root = DerefObjectStatic(Leaf(Source), CPathField("key")),
      rootSource = Source,
      expected = DerefArrayStatic(Leaf(Source), CPathIndex(0)).some
    )

    "swap positions in OuterArrayConcat" in rephraseTest(
      // rephrase([.y] ++ [.x], [.x] ++ [.y]) --> [.[1]] ++ [.[0]]
      projection = OuterArrayConcat(
        WrapArray(DerefObjectStatic(Leaf(Source), CPathField("y"))),
        WrapArray(DerefObjectStatic(Leaf(Source), CPathField("x")))
      ),
      root = OuterArrayConcat(
        WrapArray(DerefObjectStatic(Leaf(Source), CPathField("x"))),
        WrapArray(DerefObjectStatic(Leaf(Source), CPathField("y")))
      ),
      rootSource = Source,
      expected = OuterArrayConcat(
        WrapArray(DerefArrayStatic(Leaf(Source), CPathIndex(1))),
        WrapArray(DerefArrayStatic(Leaf(Source), CPathIndex(0)))
      ).some
    )

    "remap keys in OuterObjectConcat" in rephraseTest(
      // rephrase({k2: .x} ++ {k1: .y}, {k1: .x} ++ {k2: .y}) --> {k1: .k2} ++ {k2: .k1}
      projection = OuterObjectConcat(
        WrapObject(DerefObjectStatic(Leaf(Source), CPathField("x")), "k2"),
        WrapObject(DerefObjectStatic(Leaf(Source), CPathField("y")), "k1")
      ),
      root = OuterObjectConcat(
        WrapObject(DerefObjectStatic(Leaf(Source), CPathField("x")), "k1"),
        WrapObject(DerefObjectStatic(Leaf(Source), CPathField("y")), "k2")
      ),
      rootSource = Source,
      expected = OuterObjectConcat(
        WrapObject(DerefObjectStatic(Leaf(Source), CPathField("k2")), "k1"),
        WrapObject(DerefObjectStatic(Leaf(Source), CPathField("k1")), "k2")
      ).some
    )

    "remove deleted keys" in rephraseTest(
    // rephrase({key: .[0]} ++ delete({key: .[1]}, key), .[0]) --> .key
      projection = OuterObjectConcat(
        WrapObject(DerefArrayStatic(Leaf(Source), CPathIndex(0)), "key"),
        ObjectDelete(WrapObject(DerefArrayStatic(Leaf(Source), CPathIndex(1)), "key"), Set(CPathField("key")))
      ),
      root = DerefArrayStatic(Leaf(Source), CPathIndex(0)),
      rootSource = Source,
      expected = DerefObjectStatic(Leaf(Source), CPathField("key")).some
    )

    "duplicate subtrees of root in projection" in rephraseTest(
      // rephrase(.x, [.x.y] ++ [.x]) --> [.y] ++ [.]
      projection = DerefObjectStatic(Leaf(Source), CPathField("x")),
      root = OuterArrayConcat(
        WrapArray(DerefObjectStatic(DerefObjectStatic(Leaf(Source), CPathField("x")), CPathField("y"))),
        WrapArray(DerefObjectStatic(Leaf(Source), CPathField("x")))
      ),
      rootSource = Source,
      expected = OuterArrayConcat(
        WrapArray(DerefObjectStatic(Leaf(Source), CPathField("y"))),
        WrapArray(Leaf(Source))
      ).some
    )

    "select cars.name, cars2.name from cars join cars2 on cars.name = cars2.name order by cars.name" in {
      // real example
      // rephrase([[._1] ++ [._1]] ++ [[._2] ++ [._2]], _1, [.name]) --> .[0].[0].name
      // rephrase([[._1] ++ [._1]] ++ [[._2] ++ [._2]], _2, [.name]) --> .[1].[0].name
      val leftKey = WrapArray(DerefObjectStatic(Leaf(Source), CPathField("name")))
      val rightKey = WrapArray(DerefObjectStatic(Leaf(Source), CPathField("name")))
      val repair = OuterArrayConcat(
        WrapArray(OuterArrayConcat(WrapArray(Leaf(SourceLeft)), WrapArray(Leaf(SourceLeft)))),
        WrapArray(OuterArrayConcat(WrapArray(Leaf(SourceRight)), WrapArray(Leaf(SourceRight))))
      )
      val expectedLeft = Some(
        WrapArray(DerefObjectStatic(DerefArrayStatic(DerefArrayStatic(Leaf(Source), CPathIndex(0)), CPathIndex(0)), CPathField("name")))
      )
      val expectedRight = Some(
        WrapArray(DerefObjectStatic(DerefArrayStatic(DerefArrayStatic(Leaf(Source), CPathIndex(1)), CPathIndex(0)), CPathField("name")))
      )
      val resultLeft = TransSpec.rephrase(repair, SourceLeft, leftKey)
      val resultRight = TransSpec.rephrase(repair, SourceRight, rightKey)
      resultLeft mustEqual expectedLeft
      resultRight mustEqual expectedRight
    }
  }

  "normalize" should {
    import scalaz.Scalaz.ToFoldableOps, scalaz.std.list._
    def testNormalize1(in: TransSpec1, expected: TransSpec1): Result =
      TransSpec.normalize(in, TransSpec1.Undef) mustEqual expected
    def testNormalize1PE(ins: List[TransSpec1], expected: TransSpec1): Result =
      testNormalize1P(ins.map(_ -> expected))
    def testNormalize1P(inAndExpecteds: List[(TransSpec1, TransSpec1)]): Result =
      inAndExpecteds.foldMap { case (i, e) => testNormalize1(in = i, expected = e) }
    def testNormalize2(in: TransSpec2, expected: TransSpec2): Result =
      TransSpec.normalize(in, TransSpec2.Undef) mustEqual expected

    "reduce {k: .}.k to ." in testNormalize1(
      in = DerefObjectStatic(WrapObject(Leaf(Source), "k"), CPathField("k")),
      expected = Leaf(Source)
    )

    "reduce (. ++ {k: .}).k to ." in testNormalize1(
      in = DerefObjectStatic(OuterObjectConcat(Leaf(Source), WrapObject(Leaf(Source), "k")), CPathField("k")),
      expected = Leaf(Source)
    )

    "do not reduce (. ++ {k: .}).k" in {
      val in = DerefObjectStatic(OuterObjectConcat(WrapObject(Leaf(Source), "k"), Leaf(Source)), CPathField("k"))
      testNormalize1(in = in, expected = in)
    }

    "reduce ([.] ++ .).[0] to ." in testNormalize1(
      in = DerefArrayStatic(OuterArrayConcat(WrapArray(Leaf(Source)), Leaf(Source)), CPathIndex(0)),
      expected = Leaf(Source)
    )

    "reduce ([._1] ++ [._2]).[1] to ._2" in testNormalize2(
      in = DerefArrayStatic(OuterArrayConcat(WrapArray(Leaf(SourceLeft)), WrapArray(Leaf(SourceRight))), CPathIndex(1)),
      expected = Leaf(SourceRight)
    )

    "do not reduce (. ++ [.]).[0] to ." in testNormalize1(
      in = DerefArrayStatic(OuterArrayConcat(Leaf(Source), WrapArray(Leaf(Source))), CPathIndex(0)),
      expected = DerefArrayStatic(OuterArrayConcat(Leaf(Source), WrapArray(Leaf(Source))), CPathIndex(0))
    )

    "flatten nested OuterArrayConcat" in testNormalize1(
      in = OuterArrayConcat(Leaf(Source), OuterArrayConcat(Leaf(Source), OuterArrayConcat(Leaf(Source)))),
      expected = OuterArrayConcat(Leaf(Source), Leaf(Source), Leaf(Source))
    )

    "reduce [.].[1] to undefined" in testNormalize1(
      in = DerefArrayStatic(OuterArrayConcat(WrapArray(Leaf(Source))), CPathIndex(1)),
      expected = TransSpec1.Undef
    )

    "recursively reduce {k1: {k2: {k3: .}}}.k1.k2.k3 to ." in testNormalize1(
      in =
        DerefObjectStatic(
          DerefObjectStatic(
            DerefObjectStatic(
              WrapObject(WrapObject(WrapObject(Leaf(Source), "k3"), "k2"), "k1"),
              CPathField("k1")),
            CPathField("k2")),
          CPathField("k3")
        ),
      expected = Leaf(Source)
    )

    "recursively reduce {k1: {k2: {k3: .}.k3}.k2}.k1 to ." in testNormalize1(
      in = DerefObjectStatic(WrapObject(
        DerefObjectStatic(WrapObject(
          DerefObjectStatic(WrapObject(
            Leaf(Source), "k3"), CPathField("k3")),
          "k2"), CPathField("k2")),
        "k1"), CPathField("k1")
      ),
      expected = Leaf(Source)
    )

    "reduce undef.[i], undef.k to undef" in testNormalize1PE(
      ins = DerefArrayStatic(
        TransSpec1.Undef, CPathIndex(0)
      ) :: DerefObjectStatic(
        TransSpec1.Undef, CPathField("k")
      ) :: DerefArrayDynamic(
        TransSpec1.Undef, Leaf(Source)
      ) :: DerefObjectDynamic(
        TransSpec1.Undef, Leaf(Source)
      ) :: Nil,
      expected = TransSpec1.Undef
    )

    "reduce .(undef) and .[(undef)] to undef" in testNormalize1PE(
      ins = DerefArrayDynamic(
        Leaf(Source), TransSpec1.Undef
      ) :: DerefObjectDynamic(
        Leaf(Source), TransSpec1.Undef
      ) :: Nil,
      expected = TransSpec1.Undef
    )

    "normalizations must pass through all branches which are preserved" in {
      val transformers = List[TransSpec1 => TransSpec1](
        DerefArrayStatic(_, CPathIndex(1)), DerefObjectStatic(_, CPathField("k")),
        DerefMetadataStatic(_, CPathMeta("m")),
        t => DerefArrayDynamic(t, t), t => DerefObjectDynamic(t, t),
        WrapArray(_), WrapObject(_, "k"),
        t => WrapObjectDynamic(t, t),
        t => OuterObjectConcat(t, t), t => InnerObjectConcat(t, t),
        t => OuterArrayConcat(t, t), t => InnerArrayConcat(t, t),
        IsType(_, JNumberT),
        ConstLiteral(CLong(1L), _),
        t => Equal(t, t), EqualLiteral(_, CLong(1L), invert = false),
        t => Filter(t, t),
        t => Cond(t, t, t),
        t => FilterDefined(t, t, TransSpecModule.AllDefined),
        Typed(_, JNumberT),
        TypedSubsumes(_, JNumberT),
        ArraySwap(_, 1),
        Map1(_, CF1("id")(Some(_))),
        DeepMap1(_, CF1("id")(Some(_))),
        t => Map2(t, t, CF2("id")((c, _) => Some(c))),
        MapN(_, CFN("id")(_.headOption))
      )
      testNormalize1P(transformers.map(f =>
        f(DerefArrayStatic(WrapArray(Leaf(Source)), CPathIndex(0))) -> f(Leaf(Source))
      ))
    }

  }
}

object TransSpecModuleSpec extends TransSpecModuleSpec

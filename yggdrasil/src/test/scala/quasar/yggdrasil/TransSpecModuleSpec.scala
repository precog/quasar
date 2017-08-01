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
import quasar.precog.TestSupport._

trait TransSpecModuleSpec extends TransSpecModule with FNDummyModule with SpecificationLike {
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
              WrapArray(DerefArrayStatic(Leaf(Source),CPathIndex(4))),
              WrapArray(WrapObject(DerefArrayStatic(Leaf(Source),CPathIndex(6)),"baz"))),
            WrapArray(DerefArrayStatic(Leaf(Source),CPathIndex(2)))),"bar"),
          WrapObject(
            DerefArrayStatic(Leaf(Source),CPathIndex(0)),"foo"))

      result mustEqual expected
    }
  }
}

object TransSpecModuleSpec extends TransSpecModuleSpec

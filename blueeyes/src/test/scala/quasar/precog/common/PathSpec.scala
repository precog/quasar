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

package quasar.precog.common

import quasar.precog.TestSupport._

class PathSpec extends Specification with ScalaCheck {
  "rollups for a path" should {
    "not roll up when flag is false" in {
      val sample = Path("/my/fancy/path")
      sample.rollups(0) must_== List(sample)
    }

    "include the original path" in {
      val sample = Path("/my/fancy/path")
      sample.rollups(3) must containTheSameElementsAs(
        sample ::
        Path("/my/fancy") ::
        Path("/my") ::
        Path("/") :: Nil
      )
    }

    "Roll up a limited distance" in {
      val sample = Path("/my/fancy/path")
      sample.rollups(2) must containTheSameElementsAs(
        sample ::
        Path("/my/fancy") ::
        Path("/my") :: Nil
      )
    }

    "drop a matching prefix using '-'" in {
      todo
    }

    "Correctly identify child paths" in {
      val parent = Path("/my/fancy/path")
      val identical = Path("/my/fancy/path")
      val child1 = Path("/my/fancy/path/child")
      val child2 = Path("/my/fancy/path/grand/child")
      val notChild1 = Path("/other/fancy/path")
      val notChild2 = Path("/my/fancy/")

      parent.isEqualOrParentOf(parent) must beTrue
      parent.isEqualOrParentOf(identical) must beTrue
      parent.isEqualOrParentOf(child1) must beTrue
      parent.isEqualOrParentOf(child2) must beTrue

      parent.isEqualOrParentOf(notChild1) must beFalse
      parent.isEqualOrParentOf(notChild2) must beFalse

    }
  }
}

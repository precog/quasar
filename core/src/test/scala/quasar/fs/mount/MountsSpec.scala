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

package quasar.fs.mount

import slamdata.Predef.List

import pathy.Path._
import pathy.scalacheck.PathyArbitrary._
import scalaz.std.list._

class MountsSpec extends quasar.Qspec {
  "Mounts" should {
    "adding entries" >> {
      "fails when dir is a prefix of existing" >> prop { mnt: AbsDir[Sandboxed] =>
        Mounts.singleton(mnt </> dir("c1"), 1).add(mnt, 2) must be_-\/
      }

      "fails when dir is prefixed by existing" >> prop { mnt: AbsDir[Sandboxed] =>
        Mounts.singleton(mnt, 1).add(mnt </> dir("c2"), 2) must be_-\/
      }

      "succeeds when dir not a prefix of existing" >> {
        val mnt1: AbsDir[Sandboxed] = rootDir </> dir("one")
        val mnt2: AbsDir[Sandboxed] = rootDir </> dir("two")

        Mounts.fromFoldable(List((mnt1, 1), (mnt2, 2))) must be_\/-
      }

      "succeeds when replacing value at existing" >> prop { mnt: AbsDir[Sandboxed] =>
        Mounts.singleton(mnt, 1)
          .add(mnt, 2)
          .toOption
          .flatMap(_.toMap.get(mnt)) must beSome(2)
      }
    }
  }
}

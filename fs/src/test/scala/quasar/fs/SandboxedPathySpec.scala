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

package quasar.fs

import slamdata.Predef._
import quasar.contrib.pathy._, PathArbitrary._
import quasar.fs.SandboxedPathy._

import pathy.Path._
import pathy.scalacheck.PathyArbitrary._
import scalaz._, Scalaz._

class SandboxedPathySpec extends quasar.Qspec {

  "rootSubPath" should {
    "returns the correct sub path" >> prop { (d: ADir, p: RPath) =>
      refineType(rootSubPath(depth(d), d </> p)) ==== d.left
    }

    "return the path if the index is too long or the same" >> prop { (d: ADir, i: Int) =>
      (i > 0 && (i.toLong + depth(d)) < Int.MaxValue) ==> {
        refineType(rootSubPath(depth(d) + i, d)) ==== d.left
      }
    }
  }

  "largestCommonPathFromRoot" should {
    "completely different APaths should return rootDir" >> prop { (a: APath, b: APath) =>
      segAt(0, a) =/= segAt(0, b) ==> {
        refineType(largestCommonPathFromRoot(a, b)) ==== rootDir.left
      }
    }

    "return common path" >> prop { (a: ADir, b: RPath, c: RPath) =>
      segAt(0, b) =/= segAt(0, c) ==> {
        refineType(largestCommonPathFromRoot(a </> b, a </> c)) ==== a.left
      }
    }
  }

  "segAt" should {
    "segment at specified index" >> prop { (d: ADir, dirName: String, p: RPath) =>
      segAt(depth(d), d </> dir(dirName) </> p) ==== DirName(dirName).left.some
    }
  }

}

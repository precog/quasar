/*
 * Copyright 2014–2016 SlamData Inc.
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

import org.scalacheck._
import pathy.Path._
import pathy.scalacheck.PathyArbitrary._
import scalaz.std.list._
import scalaz.syntax.std.option._

trait MountsArbitrary {
  import Arbitrary.arbitrary

  implicit def mountsArbitrary[A: Arbitrary]: Arbitrary[Mounts[A]] =
    Arbitrary(for {
      n    <- Gen.size
      x    <- Gen.choose(0, n)
      dirs <- Gen.listOfN(x, arbitrary[RelDir[Sandboxed]])
      uniq =  dirs.zipWithIndex map { case (d, i) =>
                rootDir </> dir(i.toString) </> d
              }
      as   <- Gen.listOfN(x, arbitrary[A])
      mres =  Mounts.fromFoldable(uniq zip as)
      mnts <- mres.toOption.cata(Gen.const, Gen.fail)
    } yield mnts)
}

object MountsArbitrary extends MountsArbitrary

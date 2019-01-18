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

package quasar.qscript.rewrites

import quasar.Qspec
import quasar.common.CPath
import quasar.contrib.iota._
import quasar.contrib.scalaz.free._
import quasar.fp._
import quasar.qscript._

import matryoshka.data.Fix

object ProjectedSpec extends Qspec {
  import MapFuncsCore.{Add, BoolLit, MakeMap, ProjectKey, StrLit}

  val func = new construction.Func[Fix]
  val projected = Projected.unapply[Fix] _

  "find the path of a single object projection" >> {
    projected(func.ProjectKeyS(func.Hole, "xyz")) must beSome.like {
      case FreeA(p) => p must_= CPath.parse(".xyz")
    }
  }

  "find the path of a single array projection" >> {
    projected(func.ProjectIndexI(func.Hole, 7)) must beSome.like {
      case FreeA(p) => p must_= CPath.parse("[7]")
    }
  }

  "find the path of a triple object projection" >> {
    val fm =
      func.ProjectKeyS(
        func.ProjectKeyS(
          func.ProjectKeyS(
            func.Hole,
            "aaa"),
          "bbb"),
        "ccc")

    projected(fm) must beSome.like {
      case FreeA(p) => p must_= CPath.parse(".aaa.bbb.ccc")
    }
  }

  "find the path of a triple array projection" >> {
    val fm =
      func.ProjectIndexI(
        func.ProjectIndexI(
          func.ProjectIndexI(
            func.Hole,
            2),
          6),
        0)

    projected(fm) must beSome.like {
      case FreeA(p) => p must_= CPath.parse("[2][6][0]")
    }
  }

  "find the path of an array projection and object projection" >> {
    val fm =
      func.ProjectKeyS(
        func.ProjectIndexI(
          func.ProjectKeyS(
            func.Hole,
            "aaa"),
          42),
        "ccc")

    projected(fm) must beSome.like {
      case FreeA(p) => p must_= CPath.parse(".aaa[42].ccc")
    }
  }

  "find the path of a projection within an expression" >> {
    val fm =
      func.ProjectKeyS(
        func.MakeMapS(
          "map key",
          func.ProjectKeyS(func.Hole, "aaa")),
        "ccc")

    projected(fm) must beSome.like {
      case ExtractFunc(ProjectKey(
          ExtractFunc(MakeMap(StrLit("map key"), FreeA(p))),
          StrLit("ccc"))) =>
        p must_= CPath.parse(".aaa")
    }
  }

  "find multiple projections" >> {
    val fm =
      func.Add(
        func.ProjectIndexI(func.ProjectKeyS(func.Hole, "foo"), 3),
        func.ProjectKeyS(func.Hole, "bar"))

    projected(fm) must beSome.like {
      case ExtractFunc(Add(FreeA(pl), FreeA(pr))) =>
        pl must_= CPath.parse(".foo[3]")
        pr must_= CPath.parse(".bar")
    }
  }

  "fail find the path of an object projection with a non-string key" >> {
    val fm =
      func.ProjectKey(func.Hole, BoolLit(true))

    projected(fm) must beNone
  }

  "fail find the path of an object projection with a dynamic key" >> {
    val fm =
      func.ProjectKey(
        func.Hole,
        func.ToString(func.ProjectKeyS(func.Hole, "foobar")))

    projected(fm) must beNone
  }

  "fail find the path of an array projection with a dynamic key" >> {
    val fm =
      func.ProjectIndex(
        func.Hole,
        func.Integer(func.ProjectIndexI(func.Hole, 42)))

    projected(fm) must beNone
  }

  "fail find the path of an object projection with a dynamic key that is itself a projection" >> {
    val fm =
      func.ProjectKey(
        func.Hole,
        func.ProjectKeyS(func.Hole, "foobar"))

    projected(fm) must beNone
  }

  "fail find the path of an array projection with a dynamic key that is itself a projection" >> {
    val fm =
      func.ProjectIndex(
        func.Hole,
        func.ProjectIndexI(func.Hole, 42))

    projected(fm) must beNone
  }

  "fail to find the path of a projection whose source is not Hole" >> {
    val fm =
      func.ProjectKeyS(
        func.ProjectKeyS(
          func.ProjectKeyS(
            StrLit[Fix, Hole]("constant string"),
            "aaa"),
          "bbb"),
        "ccc")

    projected(fm) must beNone
  }
}

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

package quasar.frontend.logicalplan

import slamdata.Predef._
import quasar.Type
import quasar.common.PhaseResultT
import quasar.common.data.Data
import quasar.contrib.pathy._
import quasar.fp.ski.κ
import quasar.std._, StdLib._, set._, structural._, relations._

import matryoshka.data.Fix
import org.specs2.execute._
import pathy.Path.{file, rootDir}
import scalaz.\/

class EnsureCorrectTypesSpec extends quasar.Qspec with quasar.TreeMatchers {

  val lpf = new LogicalPlanR[Fix[LogicalPlan]]

  "ensureCorrectTypes" should  {

    "leave untouched if is free variable" in {
      val lp = lpf.free('sym)
      ensure(lp){ typed =>
        typed must_== lp
      }
    }

    "leave untouched if is read" in {
      val lp = lpf.read("afile")
      ensure(lp){ typed =>
        typed must_== lp
      }
    }

    "leave untouched if is let with free variables" in {
      val lp = lpf.let('__tmp0,
        lpf.free('sym),
        lpf.free('sym))
      ensure(lp){ typed =>
        typed must_== lp
      }
    }

    "leave untouched if is invoke(take) on const and free" in {
      val lp =
        lpf.let('__tmp0,
          lpf.read("afile"),
          lpf.invoke2(
            Take,
            lpf.free('__tmp0),
            lpf.constant(Data.Int(10))
          )
        )
      ensure(lp){ typed =>
        typed must_== lp
      }
    }

    "change lp for invoke with MapConcat on file values" in {
      val lp =
        lpf.let('__tmp0,
          lpf.read("afile"),
          lpf.invoke2(
            MapConcat,
            lpf.invoke2(MapProject, lpf.free('__temp0), lpf.constant(Data.Str("a"))),
            lpf.invoke2(MapProject, lpf.free('__temp0), lpf.constant(Data.Str("b")))
          )
        )
      val expected =
        lpf.let('__tmp0,
          lpf.read("afile"),
          lpf.let('__checku1,
            lpf.invoke2(MapProject, lpf.free('__temp0), lpf.constant(Data.Str("b"))),
            lpf.typecheck(
              lpf.free('__checku1),
              Type.Obj(Map(), Some(Type.Top)),
              lpf.let('__checku0,
                lpf.invoke2(MapProject, lpf.free('__temp0), lpf.constant(Data.Str("a"))),
                lpf.typecheck(
                  lpf.free('__checku0),
                  Type.Obj(Map(), Some(Type.Top)),
                  lpf.invoke2(
                    MapConcat,
                    lpf.free('__checku0),
                    lpf.free('__checku1)
                  ),
                  lpf.constant(Data.NA)
                )
              ),
              lpf.constant(Data.NA)
            )
          )
        )

      ensure(lp)(_  must beTreeEqual(expected))
    }

    /** bug #2958 */
    "contain type constraints within ifundefined" in {
      val lp =
        lpf.let('__tmp0,
          lpf.read("afile"),
          lpf.invoke2 (
            MakeMap,
            lpf.constant(Data.Str("c")),
            lpf.invoke2(
              IfUndefined,
              lpf.invoke2(
                MapConcat,
                lpf.invoke2(
                  MapProject,
                  lpf.free('__tmp0), // Map
                  lpf.constant(Data.Str("c"))),
                lpf.invoke2(
                  MapProject,
                  lpf.free('__tmp0),
                  lpf.constant(Data.Str("b"))
                )
              ),
              lpf.invoke2(MapProject, lpf.free('__tmp0), lpf.constant(Data.Str("b")))
            )
          )
        )

      val expected =
        lpf.let('__tmp0,
          lpf.let('__checku0,
            lpf.read("afile"),
            lpf.typecheck(
              lpf.free('__checku0),
              Type.Obj(Map(), Some(Type.Top)),
              lpf.free('__checku0),
              lpf.constant(Data.NA)
            )
          ),
          lpf.invoke2 (
            MakeMap,
            lpf.constant(Data.Str("c")),
            lpf.invoke2(
              IfUndefined,
              lpf.let('__checku2,
                lpf.invoke2(MapProject, lpf.free('__tmp0), lpf.constant(Data.Str("b"))),
                lpf.typecheck(
                  lpf.free('__checku2),
                  Type.Obj(Map(), Some(Type.Top)),
                  lpf.let('__checku1,
                    lpf.invoke2(MapProject, lpf.free('__tmp0), lpf.constant(Data.Str("c"))),
                    lpf.typecheck(
                      lpf.free('__checku1),
                      Type.Obj(Map(), Some(Type.Top)),
                      lpf.invoke2(MapConcat,
                        lpf.free('__checku1),
                        lpf.free('__checku2)
                      ),
                      lpf.constant(Data.NA)
                    )
                  ),
                  lpf.constant(Data.NA)
                )
              ),
              lpf.invoke2(MapProject, lpf.free('__tmp0), lpf.constant(Data.Str("b")))
            )
          )
        )
      ensure(lp)(_  must beTreeEqual(expected))
    }

  }

  private def ensure(lp: Fix[LogicalPlan])(eval: Fix[LogicalPlan] => ResultLike): Result =
    lpf.ensureCorrectTypes[PhaseResultT[ArgumentErrors \/ ?, ?]](lp)
      .value.fold(κ(failure), eval(_).toResult)

  implicit def strToFile(str: String): AFile =
    rootDir </> file(str)

}

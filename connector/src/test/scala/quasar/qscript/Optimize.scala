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

package quasar.qscript

import quasar.Predef._
import quasar.{LogicalPlan => LP, _}
import quasar.ejson.EJson
import quasar.fp._
import quasar.fs._
import quasar.sql.CompilerHelpers
import quasar.qscript.MapFuncs._

import matryoshka._, FunctorT.ops._
import pathy.Path._
import scalaz._, Scalaz._

class QScriptOptimizeSpec extends quasar.Qspec with CompilerHelpers with QScriptHelpers {
  val opt = new Optimize[Fix]

  // TODO instead of calling `.toOption` on the `\/`
  // write an `Equal[PlannerError]` and test for specific errors too
  "optimizer" should {
    "elide a no-op map in a constant boolean" in {
       val query = LP.Constant(Data.Bool(true))
       val run = liftFG(opt.elideNopMap[QS])

       QueryFile.optimizeEval(query)(run).toOption must
         equal(chain(
           RootR,
           QC.inj(Map((), BoolLit(true)))).some)
    }

    "optimize a basic read" in {
      val run =
        (SimplifyProjection[QS, QS].simplifyProjection(_: QS[Fix[QS]])) ⋙
          liftFG(opt.coalesceMapShift[QS, QS](idPrism.get)) ⋙
          Normalizable[QS].normalize ⋙
          liftFF(opt.simplifyQC[QS, QS](idPrism)) ⋙
          liftFG(opt.compactLeftShift[QS, QS])

      val query = lpRead("/foo")

      QueryFile.optimizeEval(query)(run).toOption must
      equal(chain(
        RootR,
        QC.inj(LeftShift((),
          ProjectFieldR(HoleF, StrLit("foo")),
          Free.point(RightSide)))).some)
    }

    "fold a constant array value" in {
      val value: Fix[EJson] =
        EJson.fromExt[Fix].apply(ejson.Int[Fix[EJson]](7))

      val exp: QS[Fix[QS]] =
        QC.inj(Map(
          RootR,
          Free.roll(MakeArray(Free.roll(Constant(value))))))

      val expected: QS[Fix[QS]] =
        QC.inj(Map(
          RootR,
          Free.roll(Constant(ejson.CommonEJson.inj(ejson.Arr(List(value)))))))

      exp.embed.transCata(Normalizable[QS].normalize(_: QS[Fix[QS]])) must equal(expected.embed)
    }

    "fold a constant doubly-nested array value" in {
      val value: Fix[EJson] =
        ejson.EJson.fromExt[Fix].apply(ejson.Int[Fix[EJson]](7))

      val exp: QS[Fix[QS]] =
        QC.inj(Map(
          RootR,
          Free.roll(MakeArray(Free.roll(MakeArray(Free.roll(Constant(value))))))))

      val expected: QS[Fix[QS]] =
        QC.inj(Map(
          RootR,
          Free.roll(Constant(ejson.CommonEJson.inj(ejson.Arr(List(EJson.fromCommon[Fix].apply(ejson.Arr(List(value))))))))))

      exp.embed.transCata(Normalizable[QS].normalize(_: QS[Fix[QS]])) must equal(expected.embed)
    }

    "fold nested boolean values" in {
      val falseBool: Fix[EJson] =
        EJson.fromCommon[Fix].apply(ejson.Bool[Fix[EJson]](false))

      val trueBool: Fix[EJson] =
        EJson.fromCommon[Fix].apply(ejson.Bool[Fix[EJson]](true))

      val exp: QS[Fix[QS]] =
        QC.inj(Map(
          RootR,
          Free.roll(MakeArray(
            // !false && (false || !true)
            Free.roll(And(
              Free.roll(Not(Free.roll(Constant(falseBool)))),
              Free.roll(Or(
                Free.roll(Constant(falseBool)),
                Free.roll(Not(Free.roll(Constant(trueBool))))))))))))

      val expected: QS[Fix[QS]] =
        QC.inj(Map(
          RootR,
          Free.roll(Constant(
            ejson.CommonEJson.inj(ejson.Arr(List(EJson.fromCommon[Fix].apply(ejson.Bool[Fix[ejson.EJson]](false)))))))))

      exp.embed.transCata(Normalizable[QS].normalize(_: QS[Fix[QS]])) must equal(expected.embed)
    }

    "simplify a ThetaJoin" in {
      val exp: Fix[QS] =
        TJ.inj(ThetaJoin(
          DE.inj(Const[DeadEnd, Fix[QS]](Root)).embed,
          Free.roll(R.inj(Const(Read(rootDir </> file("foo"))))),
          Free.roll(R.inj(Const(Read(rootDir </> file("bar"))))),
          Free.roll(And(Free.roll(And(
            // reversed equality
            Free.roll(Eq(
              Free.roll(ProjectField(Free.point(RightSide), StrLit("r_id"))),
              Free.roll(ProjectField(Free.point(LeftSide), StrLit("l_id"))))),
            // more complicated expression, duplicated refs
            Free.roll(Eq(
              Free.roll(Add(
                Free.roll(ProjectField(Free.point(LeftSide), StrLit("l_min"))),
                Free.roll(ProjectField(Free.point(LeftSide), StrLit("l_max"))))),
              Free.roll(Subtract(
                Free.roll(ProjectField(Free.point(RightSide), StrLit("l_max"))),
                Free.roll(ProjectField(Free.point(RightSide), StrLit("l_min"))))))))),
            // inequality
            Free.roll(Lt(
              Free.roll(ProjectField(Free.point(LeftSide), StrLit("l_lat"))),
              Free.roll(ProjectField(Free.point(RightSide), StrLit("r_lat"))))))),
          Inner,
          Free.roll(ConcatMaps(Free.point(LeftSide), Free.point(RightSide))))).embed

      exp.transCata(liftFG(opt.simplifyJoin[QS])) must equal(
        QC.inj(Map(
          QC.inj(Filter(
            EJ.inj(EquiJoin(
              DE.inj(Const[DeadEnd, Fix[QS]](Root)).embed,
              Free.roll(R.inj(Const(Read(rootDir </> file("foo"))))),
              Free.roll(R.inj(Const(Read(rootDir </> file("bar"))))),
              Free.roll(ConcatArrays(
                Free.roll(MakeArray(
                  Free.roll(ProjectField(Free.point(SrcHole), StrLit("l_id"))))),
                Free.roll(MakeArray(
                  Free.roll(Add(
                    Free.roll(ProjectField(Free.point(SrcHole), StrLit("l_min"))),
                    Free.roll(ProjectField(Free.point(SrcHole), StrLit("l_max"))))))))),
              Free.roll(ConcatArrays(
                Free.roll(MakeArray(
                  Free.roll(ProjectField(Free.point(SrcHole), StrLit("r_id"))))),
                Free.roll(MakeArray(
                  Free.roll(Subtract(
                    Free.roll(ProjectField(Free.point(SrcHole), StrLit("l_max"))),
                    Free.roll(ProjectField(Free.point(SrcHole), StrLit("l_min"))))))))),
              Inner,
              Free.roll(ConcatArrays(
                Free.roll(MakeArray(Free.point(LeftSide))),
                Free.roll(MakeArray(Free.point(RightSide))))))).embed,
            Free.roll(Lt(
              Free.roll(ProjectField(
                Free.roll(ProjectIndex(Free.point(SrcHole), IntLit(0))),
                StrLit("l_lat"))),
              Free.roll(ProjectField(
                Free.roll(ProjectIndex(Free.point(SrcHole), IntLit(1))),
                StrLit("r_lat"))))))).embed,
          Free.roll(ConcatMaps(
            Free.roll(ProjectIndex(Free.point(SrcHole), IntLit(0))),
            Free.roll(ProjectIndex(Free.point(SrcHole), IntLit(1))))))).embed)
    }
  }
}

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

package quasar.qscript

import slamdata.Predef._
import quasar._
import quasar.common.JoinType
import quasar.contrib.pathy.{AFile, ADir}
import quasar.ejson.EJson
import quasar.ejson.implicits._
import quasar.fp._
import quasar.fs._
import quasar.sql.CompilerHelpers
import quasar.qscript.MapFuncsCore._

import scala.Predef.implicitly

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import pathy.Path._
import scalaz._, Scalaz._

class QScriptRewriteSpec extends quasar.Qspec with CompilerHelpers with QScriptHelpers {
  val rewrite = new Rewrite[Fix]

  def normalizeFExpr(expr: Fix[QS]): Fix[QS] =
    expr.transCata[Fix[QS]](orOriginal(Normalizable[QS].normalizeF(_: QS[Fix[QS]])))

  def normalizeExpr(expr: Fix[QS]): Fix[QS] =
    expr.transCata[Fix[QS]](rewrite.normalize[QS])

  def simplifyJoinExpr(expr: Fix[QS]): Fix[QST] =
    expr.transCata[Fix[QST]](SimplifyJoin[Fix, QS, QST].simplifyJoin(idPrism.reverseGet))

  def compactLeftShiftExpr(expr: Fix[QS]): Fix[QS] =
    expr.transCata[Fix[QS]](
      liftFG(injectRepeatedly(rewrite.compactLeftShift[QS, QS](idPrism).apply(_: QScriptCore[Fix[QS]]))))

  def includeToExcludeExpr(expr: Fix[QST]): Fix[QST] =
    expr.transCata[Fix[QST]](
      liftFG(repeatedly(Coalesce[Fix, QST, QST].coalesceSR[QST, ADir](idPrism))) >>>
      liftFG(repeatedly(Coalesce[Fix, QST, QST].coalesceSR[QST, AFile](idPrism))))

  type QSI[A] =
    (QScriptCore :\: ProjectBucket :\: ThetaJoin :/: Const[DeadEnd, ?])#M[A]

  val DEI = implicitly[Const[DeadEnd, ?] :<: QSI]
  val QCI = implicitly[QScriptCore :<: QSI]

  val UnreferencedI: QSI[Fix[QSI]] = QCI.inj(Unreferenced[Fix, Fix[QSI]]())

  implicit def qsiToQscriptTotal: Injectable.Aux[QSI, QST] =
    ::\::[QScriptCore](::\::[ProjectBucket](::/::[Fix, ThetaJoin, Const[DeadEnd, ?]]))

  // TODO instead of calling `.toOption` on the `\/`
  // write an `Equal[PlannerError]` and test for specific errors too
  "rewriter" should {
    "elide a no-op map in a constant boolean" in {
      val query = lpf.constant(Data.Bool(true))
      val run: QSI[Fix[QSI]] => QSI[Fix[QSI]] = {
        fa => QCI.prj(fa).fold(fa)(rewrite.elideNopQC(idPrism[QSI].reverseGet))
      }

      QueryFile.convertAndNormalize[Fix, QSI](query)(run).toOption must
        equal(chain(
          UnreferencedI,
          QCI.inj(Map((), BoolLit(true)))).some)
    }

    "expand a directory read" in {
      convert(lc.some, lpRead("/foo")).flatMap(
        _.transCataM(ExpandDirs[Fix, QS, QST].expandDirs(idPrism.reverseGet, lc)).toOption.run.copoint) must
      equal(chain(
        QCT.inj(Unreferenced[Fix, Fix[QST]]()),
        QCT.inj(Union((),
          Free.roll(QST[QS].inject(QC.inj(Map(Free.roll(RTF.inj(Const[Read[AFile], FreeQS](Read(rootDir </> dir("foo") </> file("bar"))))), Free.roll(MFC(MakeMap(StrLit("bar"), HoleF))))))),
          Free.roll(QST[QS].inject(QC.inj(Union(Free.roll(QST[QS].inject(QC.inj(Unreferenced[Fix, FreeQS]()))),
            Free.roll(QST[QS].inject(QC.inj(Map(Free.roll(RTF.inj(Const[Read[AFile], FreeQS](Read(rootDir </> dir("foo") </> file("car"))))), Free.roll(MFC(MakeMap(StrLit("car"), HoleF))))))),
            Free.roll(QST[QS].inject(QC.inj(Union(Free.roll(QST[QS].inject(QC.inj(Unreferenced[Fix, FreeQS]()))),
              Free.roll(QST[QS].inject(QC.inj(Map(Free.roll(RTF.inj(Const[Read[AFile], FreeQS](Read(rootDir </> dir("foo") </> file("city"))))), Free.roll(MFC(MakeMap(StrLit("city"), HoleF))))))),
              Free.roll(QST[QS].inject(QC.inj(Union(Free.roll(QST[QS].inject(QC.inj(Unreferenced[Fix, FreeQS]()))),
                Free.roll(QST[QS].inject(QC.inj(Map(Free.roll(RTF.inj(Const[Read[AFile], FreeQS](Read(rootDir </> dir("foo") </> file("person"))))), Free.roll(MFC(MakeMap(StrLit("person"), HoleF))))))),
                Free.roll(QST[QS].inject(QC.inj(Map(Free.roll(RTF.inj(Const[Read[AFile], FreeQS](Read(rootDir </> dir("foo") </> file("zips"))))), Free.roll(MFC(MakeMap(StrLit("zips"), HoleF))))))))))))))))))))),
        QCT.inj(LeftShift((), HoleF, ExcludeId, RightSideF)))(
        implicitly, Corecursive[Fix[QST], QST]).some)
    }

    "coalesce a Map into a subsequent LeftShift" in {
      val exp: QScriptCore[Fix[QScriptCore]] =
        LeftShift(
          Map(
            Unreferenced[Fix, Fix[QScriptCore]]().embed,
            BoolLit[Fix, Hole](true)).embed,
          HoleF,
          ExcludeId,
          RightSideF)

      Coalesce[Fix, QScriptCore, QScriptCore].coalesceQC(idPrism).apply(exp) must
      equal(
        LeftShift(
          Unreferenced[Fix, Fix[QScriptCore]]().embed,
          BoolLit[Fix, Hole](true),
          ExcludeId,
          RightSideF).some)
    }

    "coalesce a Filter into a preceding ThetaJoin" in {
      val sampleFile = rootDir </> file("bar")

      val exp =
        QCT.inj(Filter(
          TJT.inj(ThetaJoin(
            QCT.inj(Unreferenced[Fix, Fix[QST]]()).embed,
            Free.roll(SRTF.inj(Const(ShiftedRead(sampleFile, IncludeId)))),
            Free.roll(SRTF.inj(Const(ShiftedRead(sampleFile, IncludeId)))),
            AndR(
              EqR(ProjectFieldR(LeftSideF, StrLit("l_id")), ProjectFieldR(RightSideF, StrLit("r_id"))),
              EqR(
                AddR(
                  ProjectFieldR(LeftSideF, StrLit("l_min")),
                  ProjectFieldR(LeftSideF, StrLit("l_max"))),
                SubtractR(
                  ProjectFieldR(RightSideF, StrLit("l_max")),
                  ProjectFieldR(RightSideF, StrLit("l_min"))))),
            JoinType.Inner,
            ConcatMapsR(
              MakeMapR(StrLit("l"), LeftSideF),
              MakeMapR(StrLit("r"), RightSideF)))).embed,
            LtR(
              ProjectFieldR(
                ProjectFieldR(HoleF, StrLit("l")),
                StrLit("lat")),
              ProjectFieldR(
                ProjectFieldR(HoleF, StrLit("l")),
                StrLit("lon")))))

      Coalesce[Fix, QST, QST].coalesceTJ(idPrism[QST].get).apply(exp).map(rewrite.normalize[QST]) must
      equal(
        TJT.inj(ThetaJoin(
          QCT.inj(Unreferenced[Fix, Fix[QST]]()).embed,
          Free.roll(SRTF.inj(Const(ShiftedRead(sampleFile, IncludeId)))),
          Free.roll(SRTF.inj(Const(ShiftedRead(sampleFile, IncludeId)))),
          AndR(
            AndR(
              EqR(ProjectFieldR(LeftSideF, StrLit("l_id")), ProjectFieldR(RightSideF, StrLit("r_id"))),
              EqR(
                AddR(
                  ProjectFieldR(LeftSideF, StrLit("l_min")),
                  ProjectFieldR(LeftSideF, StrLit("l_max"))),
                SubtractR(
                  ProjectFieldR(RightSideF, StrLit("l_max")),
                  ProjectFieldR(RightSideF, StrLit("l_min"))))),
            LtR(
              ProjectFieldR(LeftSideF, StrLit("lat")),
              ProjectFieldR(LeftSideF, StrLit("lon")))),
          JoinType.Inner,
          ConcatMapsR(
            MakeMapR(StrLit("l"), LeftSideF),
            MakeMapR(StrLit("r"), RightSideF)))).some)

    }

    "fold a constant array value" in {
      val value: Fix[EJson] =
        EJson.fromExt(ejson.Int[Fix[EJson]](7))

      val exp: QS[Fix[QS]] =
        QC.inj(Map(
          RootR.embed,
          MakeArrayR(IntLit(7))))

      val expected: QS[Fix[QS]] =
        QC.inj(Map(
          RootR.embed,
          ConstantR(ejson.CommonEJson.inj(ejson.Arr(List(value))).embed)))

      normalizeFExpr(exp.embed) must equal(expected.embed)
    }

    "elide a join with a constant on one side" in {
      val exp: QS[Fix[QS]] =
        TJ.inj(ThetaJoin(
          RootR.embed,
          Free.roll(QCT.inj(LeftShift(
            Free.roll(QCT.inj(Map(
              Free.roll(DET.inj(Const[DeadEnd, FreeQS](Root))),
              ProjectFieldR(HoleF, StrLit("city"))))),
            HoleF,
            IncludeId,
            ConcatArraysR(
              MakeArrayR(LeftSideF),
              MakeArrayR(RightSideF))))),
          Free.roll(QCT.inj(Map(
            Free.roll(QCT.inj(Unreferenced[Fix, FreeQS]())),
            StrLit("name")))),
          BoolLit[Fix, JoinSide](true),
          JoinType.Inner,
          ProjectFieldR(
            ProjectIndexR(
              ProjectIndexR(LeftSideF, IntLit(1)),
              IntLit(1)),
            RightSideF)))

      // TODO: only require a single pass
      normalizeExpr(normalizeExpr(exp.embed)) must equal(
        chain(
          RootR,
          QC.inj(LeftShift((),
            ProjectFieldR(HoleF, StrLit("city")),
            ExcludeId,
            ProjectFieldR(RightSideF, StrLit("name"))))))
    }

    "fold a constant doubly-nested array value" in {
      val value: Fix[EJson] =
        EJson.fromExt(ejson.Int[Fix[EJson]](7))

      val exp: QS[Fix[QS]] =
        QC.inj(Map(
          RootR.embed,
          MakeArrayR(MakeArrayR(IntLit(7)))))

      val expected: QS[Fix[QS]] =
        QC.inj(Map(
          RootR.embed,
          ConstantR(ejson.CommonEJson.inj(ejson.Arr(List(EJson.fromCommon(ejson.Arr(List(value)))))).embed)))

      normalizeFExpr(exp.embed) must equal(expected.embed)
    }

    "elide a join in the branch of a join" in {
      val exp: QS[Fix[QS]] =
        TJ.inj(ThetaJoin(
          RootR.embed,
          Free.roll(QCT.inj(Map(
            Free.roll(QCT.inj(Unreferenced())),
            StrLit("name")))),
          Free.roll(TJT.inj(ThetaJoin(
            Free.roll(DET.inj(Const(Root))),
            Free.roll(QCT.inj(LeftShift(
              HoleQS,
              HoleF,
              IncludeId,
              ConcatArraysR(
                MakeArrayR(LeftSideF),
                MakeArrayR(RightSideF))))),
            Free.roll(QCT.inj(Map(
              Free.roll(QCT.inj(Unreferenced())),
              StrLit("name")))),
            BoolLit(true),
            JoinType.Inner,
            ConcatArraysR(
              MakeArrayR(LeftSideF),
              MakeArrayR(RightSideF))))),
          BoolLit(true),
          JoinType.Inner,
          ConcatArraysR(
            MakeArrayR(LeftSideF),
            MakeArrayR(RightSideF))))

      // TODO: only require a single pass
      normalizeExpr(normalizeExpr(exp.embed)) must
        equal(
          QC.inj(LeftShift(
            RootR.embed,
            HoleF,
            IncludeId,
            ConcatArraysR(
              ConstantR(ejson.CommonEJson.inj(ejson.Arr(List(EJson.fromCommon(ejson.Str[Fix[ejson.EJson]]("name"))))).embed),
              MakeArrayR(
                ConcatArraysR(
                  MakeArrayR(
                    ConcatArraysR(
                      MakeArrayR(LeftSideF),
                      MakeArrayR(RightSideF))),
                  ConstantR(ejson.CommonEJson.inj(ejson.Arr(List(EJson.fromCommon(ejson.Str[Fix[ejson.EJson]]("name"))))).embed)))))).embed)
    }

    "fold nested boolean values" in {
      val exp: QS[Fix[QS]] =
        QC.inj(Map(
          RootR.embed,
          MakeArrayR(
            // !false && (false || !true)
            AndR(
              NotR(BoolLit(false)),
              OrR(
                BoolLit(false),
                NotR(BoolLit(true)))))))

      val expected: QS[Fix[QS]] =
        QC.inj(Map(
          RootR.embed,
          ConstantR(ejson.CommonEJson.inj(ejson.Arr(List(EJson.fromCommon(ejson.Bool[Fix[ejson.EJson]](false))))).embed)))

      normalizeFExpr(exp.embed) must equal(expected.embed)
    }

    "simplify a ThetaJoin" in {
      val exp: QS[Fix[QS]] =
        TJ.inj(ThetaJoin(
          QC.inj(Unreferenced[Fix, Fix[QS]]()).embed,
          Free.roll(RTF.inj(Const(Read(rootDir </> file("foo"))))),
          Free.roll(RTF.inj(Const(Read(rootDir </> file("bar"))))),
          AndR(AndR(
            // reversed equality
            EqR(
              ProjectFieldR(RightSideF, StrLit("r_id")),
              ProjectFieldR(LeftSideF, StrLit("l_id"))),
            // more complicated expression, duplicated refs
            EqR(
              AddR(
                ProjectFieldR(LeftSideF, StrLit("l_min")),
                ProjectFieldR(LeftSideF, StrLit("l_max"))),
              SubtractR(
                ProjectFieldR(RightSideF, StrLit("l_max")),
                ProjectFieldR(RightSideF, StrLit("l_min"))))),
            // inequality
            LtR(
              ProjectFieldR(LeftSideF, StrLit("l_lat")),
              ProjectFieldR(RightSideF, StrLit("r_lat")))),
          JoinType.Inner,
          ConcatMapsR(LeftSideF, RightSideF)))

      simplifyJoinExpr(exp.embed) must equal(
        QCT.inj(Map(
          QCT.inj(Filter(
            EJT.inj(EquiJoin(
              QCT.inj(Unreferenced[Fix, Fix[QST]]()).embed,
              Free.roll(RTF.inj(Const(Read(rootDir </> file("foo"))))),
              Free.roll(RTF.inj(Const(Read(rootDir </> file("bar"))))),
              ConcatArraysR(
                MakeArrayR(
                  ProjectFieldR(HoleF, StrLit("l_id"))),
                MakeArrayR(
                  AddR(
                    ProjectFieldR(HoleF, StrLit("l_min")),
                    ProjectFieldR(HoleF, StrLit("l_max"))))),
              ConcatArraysR(
                MakeArrayR(
                  ProjectFieldR(HoleF, StrLit("r_id"))),
                MakeArrayR(
                  SubtractR(
                    ProjectFieldR(HoleF, StrLit("l_max")),
                    ProjectFieldR(HoleF, StrLit("l_min"))))),
              JoinType.Inner,
              ConcatArraysR(
                MakeArrayR(LeftSideF),
                MakeArrayR(RightSideF)))).embed,
            LtR(
              ProjectFieldR(
                ProjectIndexR(HoleF, IntLit(0)),
                StrLit("l_lat")),
              ProjectFieldR(
                ProjectIndexR(HoleF, IntLit(1)),
                StrLit("r_lat"))))).embed,
          ConcatMapsR(
            ProjectIndexR(HoleF, IntLit(0)),
            ProjectIndexR(HoleF, IntLit(1))))).embed)
    }

    "transform a ShiftedRead with IncludeId to ExcludeId when possible" in {
      val sampleFile = rootDir </> file("bar")

      val originalQScript =
        QCT.inj(Map(
          SRTF.inj(Const[ShiftedRead[AFile], Fix[QST]](ShiftedRead(sampleFile, IncludeId))).embed,
          AddR(
            ProjectIndexR(HoleF, IntLit(1)),
            ProjectIndexR(HoleF, IntLit(1))))).embed

      val expectedQScript =
        QCT.inj(Map(
          SRTF.inj(Const[ShiftedRead[AFile], Fix[QST]](ShiftedRead(sampleFile, ExcludeId))).embed,
          AddR(HoleF, HoleF))).embed

      includeToExcludeExpr(originalQScript) must_= expectedQScript
    }

    "transform a left shift with a static array as the source" in {
      val original: Fix[QS] =
        QC.inj(LeftShift(
          QC.inj(Map(
            RootR.embed,
            MakeArrayR(AddR(HoleF, IntLit(3))))).embed,
          HoleF,
          ExcludeId,
          ConcatMapsR(
            MakeMapR(StrLit("right"), RightSideF),
            MakeMapR(StrLit("left"), LeftSideF)))).embed

      val expected: Fix[QS] =
        QC.inj(Map(
          RootR.embed,
          ConcatMapsR(
            MakeMapR(StrLit("right"), AddR(HoleF, IntLit(3))),
            MakeMapR(StrLit("left"), MakeArrayR(AddR(HoleF, IntLit(3))))))).embed

      compactLeftShiftExpr(original) must equal(expected)
    }

    "transform a left shift with a static array as the struct" in {
      val original: Fix[QS] =
        QC.inj(LeftShift(
          QC.inj(Map(
            RootR.embed,
            AddR(HoleF, IntLit(3)))).embed,
          MakeArrayR(SubtractR(HoleF, IntLit(5))),
          ExcludeId,
          ConcatMapsR(
            MakeMapR(StrLit("right"), RightSideF),
            MakeMapR(StrLit("left"), LeftSideF)))).embed

      val expected: Fix[QS] =
        QC.inj(Map(
          QC.inj(Map(
            RootR.embed,
            AddR(HoleF, IntLit(3)))).embed,
          ConcatMapsR(
            MakeMapR(StrLit("right"), SubtractR(HoleF, IntLit(5))),
            MakeMapR(StrLit("left"), HoleF)))).embed

      compactLeftShiftExpr(original) must equal(expected)
    }
  }
}

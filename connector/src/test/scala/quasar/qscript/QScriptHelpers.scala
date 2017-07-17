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
import quasar.common.{PhaseResult, PhaseResults, PhaseResultT}
import quasar.contrib.pathy._
import quasar.contrib.scalaz.eitherT._
import quasar.ejson, ejson.EJson
import quasar.ejson.implicits._
import quasar.fp._
import quasar.fs._
import quasar.frontend.logicalplan.{LogicalPlan => LP}
import quasar.sql.CompilerHelpers

import scala.Predef.implicitly

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import pathy.Path._
import scalaz._, Scalaz._

trait QScriptHelpers extends CompilerHelpers with TTypes[Fix] {
  type QS[A] = (
    QScriptCore           :\:
    ThetaJoin             :\:
    Const[Read[ADir], ?]  :\:
    Const[Read[AFile], ?] :/:
    Const[DeadEnd, ?]
  )#M[A]

  val DE = implicitly[Const[DeadEnd, ?]     :<: QS]
  val RD = implicitly[Const[Read[ADir], ?]  :<: QS]
  val RF = implicitly[Const[Read[AFile], ?] :<: QS]
  val QC = implicitly[QScriptCore           :<: QS]
  val TJ = implicitly[ThetaJoin             :<: QS]

  implicit val QS: Injectable.Aux[QS, QST] =
    ::\::[QScriptCore](
      ::\::[ThetaJoin](
        ::\::[Const[Read[ADir], ?]](
          ::/::[Fix, Const[Read[AFile], ?], Const[DeadEnd, ?]])))

  val RootR: QS[Fix[QS]] = DE.inj(Const[DeadEnd, Fix[QS]](Root))
  val UnreferencedR: QS[Fix[QS]] = QC.inj(Unreferenced[Fix, Fix[QS]]())
  def ReadR(path: APath): QS[Fix[QS]] =
    refineType(path).fold(
      d => RD.inj(Const(Read(d))),
      f => RF.inj(Const(Read(f))))

  type QST[A] = QScriptTotal[A]

  def QST[F[_]](implicit ev: Injectable.Aux[F, QST]) = ev

  val DET  =            implicitly[Const[DeadEnd, ?] :<: QST]
  val RTD  =        implicitly[Const[Read[ADir], ?]  :<: QST]
  val RTF  =        implicitly[Const[Read[AFile], ?] :<: QST]
  val QCT  =                  implicitly[QScriptCore :<: QST]
  val TJT  =                    implicitly[ThetaJoin :<: QST]
  val EJT  =                     implicitly[EquiJoin :<: QST]
  val PBT  =                implicitly[ProjectBucket :<: QST]
  val SRTD = implicitly[Const[ShiftedRead[ADir], ?]  :<: QST]
  val SRTF = implicitly[Const[ShiftedRead[AFile], ?] :<: QST]

  val RootRT: QST[Fix[QST]] = DET.inj(Const[DeadEnd, Fix[QST]](Root))
  val UnreferencedRT: QST[Fix[QST]] = QCT.inj(Unreferenced[Fix, Fix[QST]]())
  def ReadRT(file: AFile): QST[Fix[QST]] = RTF.inj(Const(Read(file)))

  def ConstantR[A](value: Fix[EJson]):
      FreeMapA[A] =
    Free.roll(MFC(MapFuncsCore.Constant[Fix, FreeMapA[A]](value)))

  def ProjectFieldR[A](src: FreeMapA[A], field: FreeMapA[A]):
      FreeMapA[A] =
    Free.roll(MFC(MapFuncsCore.ProjectField(src, field)))

  def DeleteFieldR[A](src: FreeMapA[A], field: FreeMapA[A]):
      FreeMapA[A] =
    Free.roll(MFC(MapFuncsCore.DeleteField(src, field)))

  def ProjectIndexR[A](src: FreeMapA[A], field: FreeMapA[A]):
      FreeMapA[A] =
    Free.roll(MFC(MapFuncsCore.ProjectIndex(src, field)))

  def MakeArrayR[A](src: FreeMapA[A]):
      FreeMapA[A] =
    Free.roll(MFC(MapFuncsCore.MakeArray(src)))

  def MakeMapR[A](key: FreeMapA[A], src: FreeMapA[A]):
      FreeMapA[A] =
    Free.roll(MFC(MapFuncsCore.MakeMap(key, src)))

  def ConcatArraysR[A](left: FreeMapA[A], right: FreeMapA[A]):
      FreeMapA[A] =
    Free.roll(MFC(MapFuncsCore.ConcatArrays(left, right)))

  def ConcatMapsR[A](left: FreeMapA[A], right: FreeMapA[A]):
      FreeMapA[A] =
    Free.roll(MFC(MapFuncsCore.ConcatMaps(left, right)))

  def AddR[A](left: FreeMapA[A], right: FreeMapA[A]):
      FreeMapA[A] =
    Free.roll(MFC(MapFuncsCore.Add(left, right)))

  def SubtractR[A](left: FreeMapA[A], right: FreeMapA[A]):
      FreeMapA[A] =
    Free.roll(MFC(MapFuncsCore.Subtract(left, right)))

  def EqR[A](left: FreeMapA[A], right: FreeMapA[A]):
      FreeMapA[A] =
    Free.roll(MFC(MapFuncsCore.Eq(left, right)))

  def LtR[A](left: FreeMapA[A], right: FreeMapA[A]):
      FreeMapA[A] =
    Free.roll(MFC(MapFuncsCore.Lt(left, right)))

  def AndR[A](left: FreeMapA[A], right: FreeMapA[A]):
      FreeMapA[A] =
    Free.roll(MFC(MapFuncsCore.And(left, right)))

  def OrR[A](left: FreeMapA[A], right: FreeMapA[A]):
      FreeMapA[A] =
    Free.roll(MFC(MapFuncsCore.Or(left, right)))

  def NotR[A](value: FreeMapA[A]):
      FreeMapA[A] =
    Free.roll(MFC(MapFuncsCore.Not(value)))

  def lpRead(path: String): Fix[LP] =
    lpf.read(unsafeSandboxAbs(posixCodec.parseAbsFile(path).get))

  val prov = new provenance.ProvenanceT[Fix]

  /** A helper when writing examples that allows them to be written in order of
    * execution.
    */
  // NB: Would prefer this to be `ops: (T => F[T])*`, but it makes the call
  //     site too annotate-y.
  // FIXME: The `Corecursive` implicit here isn’t resolved unless there are
  //        _exactly_ two arguments passed to the function. When this is fixed,
  //        remove the implicit lists from all of the call sites.
  def chain[T, F[_]: Functor]
    (op: F[T], ops: F[Unit]*)
    (implicit T: Corecursive.Aux[T, F])
      : T =
    ops.foldLeft(op.embed)((acc, elem) => elem.as(acc).embed)

  val listContents: DiscoverPath.ListContents[Id] =
    d =>
      if (d ≟ rootDir)
        Set(
          DirName("foo").left,
          DirName("some").left,
          FileName("bar").right,
          FileName("city").right,
          FileName("person").right,
          FileName("zips").right,
          FileName("car").right)
      else if (d ≟ (rootDir </> dir("some")))
        Set(
          DirName("foo").left,
          FileName("bar").right,
          FileName("city").right,
          FileName("person").right,
          FileName("zips").right,
          FileName("car").right)
      else
        Set(
          FileName("bar").right[DirName],
          FileName("city").right,
          FileName("person").right,
          FileName("zips").right,
          FileName("car").right)

  def lc = listContents >>> (_.point[FileSystemErrT[PhaseResultT[Id, ?], ?]])

  implicit val monadTell: MonadTell[FileSystemErrT[PhaseResultT[Id, ?], ?], PhaseResults] =
    EitherT.monadListen[WriterT[Id, Vector[PhaseResult], ?], PhaseResults, FileSystemError](
      WriterT.writerTMonadListen[Id, Vector[PhaseResult]])

  def convert(lc: Option[DiscoverPath.ListContents[FileSystemErrT[PhaseResultT[Id, ?], ?]]], lp: Fix[LP]):
      Option[Fix[QS]] =
    lc.fold(
      QueryFile.convertToQScript[Fix, QS](lp))(
      QueryFile.convertToQScriptRead[Fix, FileSystemErrT[PhaseResultT[Id, ?], ?], QS](_)(lp))
      .toOption.run.copoint

  val ejsonNull =
    ejson.CommonEJson(ejson.Null[Fix[EJson]]()).embed

  def ejsonInt(int: Int) =
    ejson.ExtEJson(ejson.Int[Fix[EJson]](int)).embed

  def ejsonStr(str: String) =
    ejson.CommonEJson(ejson.Str[Fix[EJson]](str)).embed

  def ejsonArr(elems: Fix[EJson]*) =
    ejson.CommonEJson(ejson.Arr(elems.toList)).embed

  def ejsonMap(elems: (Fix[EJson], Fix[EJson])*) =
    ejson.ExtEJson(ejson.Map(elems.toList)).embed

  val ejsonNullArr =
    ejsonArr(ejson.CommonEJson(ejson.Null[Fix[EJson]]()).embed)

  def ejsonJoin(l: Fix[EJson], r: Fix[EJson]) =
    ejsonMap((
      ejson.CommonEJson(ejson.Str[Fix[EJson]]("j")).embed,
      ejsonArr(l, r)))

  def ejsonProjectField(field: Fix[EJson]) =
    ejsonMap((ejson.CommonEJson(ejson.Str[Fix[EJson]]("f")).embed, field))
}

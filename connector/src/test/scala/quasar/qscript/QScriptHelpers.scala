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
import quasar.ejson, ejson.{EJson, Fixed}
import quasar.ejson.implicits._
import quasar.fp._
import quasar.fs._
import quasar.frontend.logicalplan.{LogicalPlan => LP}
import quasar.sql.CompilerHelpers

import scala.Predef.implicitly

import matryoshka._
import matryoshka.data.Fix
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

  implicit def qScriptCoreToQScript: Injectable.Aux[QScriptCore, QS] =
    Injectable.inject[QScriptCore, QS]

  implicit def thetaJoinToQScript: Injectable.Aux[ThetaJoin, QS] =
    Injectable.inject[ThetaJoin, QS]

  implicit def readFileToQScript: Injectable.Aux[Const[Read[AFile], ?], QS] =
    Injectable.inject[Const[Read[AFile], ?], QS]

  implicit def readDirToQScript: Injectable.Aux[Const[Read[ADir], ?], QS] =
    Injectable.inject[Const[Read[ADir], ?], QS]

  val qsdsl = construction.mkDefaults[Fix, QS]
  val qstdsl = construction.mkDefaults[Fix, QST]
  val json = Fixed[Fix[EJson]]

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

  def lpRead(path: String): Fix[LP] =
    lpf.read(unsafeSandboxAbs(posixCodec.parseAbsFile(path).get))

  val prov = new provenance.ProvenanceT[Fix]

  /** A helper when writing examples that allows them to be written in order of
    * execution.
    */
  // NB: Would prefer this to be generic over `Fix[QS]`, but it makes the call
  //     site too annotate-y.
  def chainQS
  (op: Fix[QS], ops: (Fix[QS] => Fix[QS])*)
  : Fix[QS] =
    ops.foldLeft(op)((acc, elem) => elem(acc))

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

}

object QScriptHelpers extends QScriptHelpers

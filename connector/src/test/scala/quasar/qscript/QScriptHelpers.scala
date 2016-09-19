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
import quasar.{LogicalPlan => LP, PhaseResultT}
import quasar.contrib.pathy._
import quasar.fp._, eitherT._
import quasar.fs._
import quasar.qscript.MapFuncs._

import scala.Predef.implicitly

import matryoshka._
import pathy.Path._
import scalaz._, Scalaz._

trait QScriptHelpers {
  // TODO: Narrow this to QScriptPure
  type QS[A] = QScriptTotal[Fix, A]
  val DE = implicitly[Const[DeadEnd, ?] :<: QS]
  val R  = implicitly[Const[Read, ?] :<: QS]
  val SR = implicitly[Const[ShiftedRead, ?] :<: QS]
  val QC = implicitly[QScriptCore[Fix, ?] :<: QS]
  val EJ = implicitly[EquiJoin[Fix, ?] :<: QS]
  val TJ = implicitly[ThetaJoin[Fix, ?] :<: QS]

  def RootR: QS[Fix[QS]] = DE.inj(Const[DeadEnd, Fix[QS]](Root))
  def UnreferencedR: QS[Fix[QS]] = QC.inj(Unreferenced[Fix, Fix[QS]]())
  def ReadR(file: AFile): QS[Fix[QS]] = R.inj(Const[Read, Fix[QS]](Read(file)))

  def ProjectFieldR[A](
    src: Free[MapFunc[Fix, ?], A], field: Free[MapFunc[Fix, ?], A]):
      Free[MapFunc[Fix, ?], A] =
    Free.roll(ProjectField(src, field))

  def lpRead(path: String): Fix[LP] =
    LP.Read(sandboxAbs(posixCodec.parseAbsFile(path).get))

  /** A helper when writing examples that allows them to be written in order of
    * execution.
    */
  // NB: Would prefer this to be `ops: T[F] => F[T[F]]*`, but it makes the call
  //     site too annotate-y.
  def chain[T[_[_]]: Corecursive, F[_]: Functor](op: F[T[F]], ops: F[Unit]*):
      T[F] =
    ops.foldLeft(op.embed)((acc, elem) => elem.as(acc).embed)

  val listContents: ConvertPath.ListContents[Id] =
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

  def convert(lc: Option[ConvertPath.ListContents[Id]], lp: Fix[LP]):
      Option[Fix[QScriptTotal[Fix, ?]]] = {
    val lc1 = lc map (_.andThen(_.liftM[PhaseResultT].liftM[FileSystemErrT]))
    QueryFile.convertToQScript(lc1)(lp).toOption.run.copoint
  }
}

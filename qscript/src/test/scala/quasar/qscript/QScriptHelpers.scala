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

package quasar.qscript

import quasar.contrib.pathy._
import quasar.ejson, ejson.{EJson, Fixed}
import quasar.fp._

import scala.Predef.implicitly

import matryoshka._
import matryoshka.data.Fix
import scalaz._

trait QScriptHelpers extends TTypes[Fix] {
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

  val qsdsl = construction.mkDefaults[Fix, QS]
  val qstdsl = construction.mkDefaults[Fix, QST]
  val json = Fixed[Fix[EJson]]

  /** A helper when writing examples that allows them to be written in order of
    * execution.
    */
  def chainQS
  (op: Fix[QS], ops: (Fix[QS] => Fix[QS])*)
  : Fix[QS] =
    ops.foldLeft(op)((acc, elem) => elem(acc))
}

object QScriptHelpers extends QScriptHelpers

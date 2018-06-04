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

package quasar.physical

import slamdata.Predef.String
import quasar.contrib.iota.SubInject
import quasar.qscript._
import quasar.contrib.scalaz.MonadError_
import quasar.contrib.pathy.{ADir, AFile}
import quasar.fp.Injectable

import scalaz.{Const, NonEmptyList, MonadError}

import iotaz.CopK

package object marklogic {
  type ErrorMessages = NonEmptyList[String]

  type MonadErrMsgs[F[_]]  = MonadError[F, ErrorMessages]

  object MonadErrMsgs {
    def apply[F[_]](implicit F: MonadErrMsgs[F]): MonadErrMsgs[F] = F
  }

  type MonadErrMsgs_[F[_]] = MonadError_[F, ErrorMessages]

  object MonadErrMsgs_ {
    def apply[F[_]](implicit F: MonadErrMsgs_[F]): MonadErrMsgs_[F] = F
  }

  implicit def qScriptToQScriptTotal[T[_[_]]]: Injectable[CopK[fs.MLQScriptCP[T], ?], QScriptTotal[T, ?]] =
    SubInject[CopK[fs.MLQScriptCP[T], ?], QScriptTotal[T, ?]]
  
  implicit def qScriptCoreToQScript[T[_[_]]]: Injectable[QScriptCore[T, ?], CopK[fs.MLQScriptCP[T], ?]] =
    Injectable.inject[QScriptCore[T, ?], CopK[fs.MLQScriptCP[T], ?]]

  implicit def thetaJoinToQScript[T[_[_]]]: Injectable[ThetaJoin[T, ?], CopK[fs.MLQScriptCP[T], ?]] =
    Injectable.inject[ThetaJoin[T, ?], CopK[fs.MLQScriptCP[T], ?]]

  implicit def readToQScript[T[_[_]]]: Injectable[Const[Read[AFile], ?], CopK[fs.MLQScriptCP[T], ?]] =
    Injectable.inject[Const[Read[AFile], ?], CopK[fs.MLQScriptCP[T], ?]]

  implicit def shiftedReadToQScript[T[_[_]]]: Injectable[Const[ShiftedRead[ADir], ?], CopK[fs.MLQScriptCP[T], ?]] =
    Injectable.inject[Const[ShiftedRead[ADir], ?], CopK[fs.MLQScriptCP[T], ?]]


}

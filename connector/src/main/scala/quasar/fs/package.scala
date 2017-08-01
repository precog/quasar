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

package quasar

import quasar.contrib.scalaz.MonadError_
import quasar.effect.Failure
import quasar.fp._
import quasar.fp.free._

import scalaz.{Failure => _, _}

package object fs extends PhysicalErrorPrisms {
  type FileSystem[A] = (QueryFile :\: ReadFile :\: WriteFile :/: ManageFile)#M[A]

  type BackendEffect[A] = Coproduct[Analyze, FileSystem, A]

  type FileSystemFailure[A] = Failure[FileSystemError, A]
  type FileSystemErrT[F[_], A] = EitherT[F, FileSystemError, A]

  type MonadFsErr[F[_]] = MonadError_[F, FileSystemError]

  object MonadFsErr {
    def apply[F[_]](implicit F: MonadFsErr[F]): MonadFsErr[F] = F
  }

  type PhysErr[A] = Failure[PhysicalError, A]

  def interpretFileSystem[M[_]](
    q: QueryFile ~> M,
    r: ReadFile ~> M,
    w: WriteFile ~> M,
    m: ManageFile ~> M
  ): FileSystem ~> M =
    q :+: r :+: w :+: m

  def interpretBackendEffect[M[_]](
    a: Analyze ~> M,
    q: QueryFile ~> M,
    r: ReadFile ~> M,
    w: WriteFile ~> M,
    m: ManageFile ~> M
  ): BackendEffect ~> M =
    a :+: q :+: r :+: w :+: m

}

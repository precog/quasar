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

package quasar.fs

import slamdata.Predef.{Map, Vector}
import quasar.contrib.pathy._
import quasar.effect._
import quasar.fp._, free._

import monocle.Lens
import scalaz.{Lens => _, Failure => _, _}
import scalaz.concurrent.Task

package object mount {
  type MntErrT[F[_], A] = EitherT[F, MountingError, A]

  type MountingFailure[A] = Failure[MountingError, A]

  object MountingFailure {
    def Ops[S[_]](implicit S: MountingFailure :<: S) =
      Failure.Ops[MountingError, S]
  }

  type PathMismatchFailure[A] = Failure[Mounting.PathTypeMismatch, A]

  object PathMismatchFailure {
    def Ops[S[_]](implicit S: PathMismatchFailure :<: S) =
      Failure.Ops[Mounting.PathTypeMismatch, S]
  }

  type MountConfigs[A] = KeyValueStore[APath, MountConfig, A]

  object MountConfigs {
    def Ops[S[_]](implicit S: MountConfigs :<: S) =
      KeyValueStore.Ops[APath, MountConfig, S]
  }


  //-- Views --

  sealed abstract class ResultSet

  object ResultSet {
    final case class Data(values: Vector[quasar.Data])       extends ResultSet
    final case class Read(handle: ReadFile.ReadHandle)       extends ResultSet
    final case class Results(handle: QueryFile.ResultHandle) extends ResultSet
  }

  type ViewState[A] = KeyValueStore[ReadFile.ReadHandle, ResultSet, A]

  object ViewState {
    def Ops[S[_]](
      implicit S: ViewState :<: S
    ): KeyValueStore.Ops[ReadFile.ReadHandle, ResultSet, S] =
      KeyValueStore.Ops[ReadFile.ReadHandle, ResultSet, S]

    type ViewHandles = Map[ReadFile.ReadHandle, ResultSet]

    def toTask(initial: ViewHandles): Task[ViewState ~> Task] =
      TaskRef(initial) map KeyValueStore.impl.fromTaskRef

    def toState[S](l: Lens[S, ViewHandles]): ViewState ~> State[S, ?] =
      KeyValueStore.impl.toState[State[S,?]](l)
  }

  type ViewFileSystem[A] = (
        Mounting
    :\: PathMismatchFailure
    :\: MountingFailure
    :\: ViewState
    :\: MonotonicSeq
    :/: FileSystem
  )#M[A]

  object ViewFileSystem {
    def interpret[F[_]](
      mounting: Mounting ~> F,
      mismatchFailure: PathMismatchFailure ~> F,
      mountingFailure: MountingFailure ~> F,
      viewState: ViewState ~> F,
      monotonicSeq: MonotonicSeq ~> F,
      fileSystem: FileSystem ~> F
    ): ViewFileSystem ~> F =
      mounting :+: mismatchFailure :+: mountingFailure :+: viewState :+: monotonicSeq :+: fileSystem
  }
}

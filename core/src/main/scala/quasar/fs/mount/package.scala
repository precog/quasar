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

package quasar.fs

import quasar.Predef.Map
import quasar.effect._
import quasar.fp.{TaskRef}
import quasar.fp.free, free._

import scala.collection.immutable.Vector

import monocle.Lens
import scalaz.{Lens => _, _}
import scalaz.concurrent.Task

package object mount {
  type MntErrT[F[_], A] = EitherT[F, MountingError, A]

  type MountConfigs[A] = KeyValueStore[APath, MountConfig, A]

  type MountingFileSystem[A] = Coproduct[Mounting, FileSystem, A]

  def interpretMountingFileSystem[M[_]](
    m: Mounting ~> M,
    fs: FileSystem ~> M
  ): MountingFileSystem ~> M =
    m :+: fs

  //-- Views --

  sealed trait ResultSet
  object ResultSet {
    final case class Data(values: Vector[quasar.Data])       extends ResultSet
    final case class Read(handle: ReadFile.ReadHandle)       extends ResultSet
    final case class Results(handle: QueryFile.ResultHandle) extends ResultSet
  }

  type ViewHandles = Map[ReadFile.ReadHandle, ResultSet]

  type ViewState[A] = KeyValueStore[ReadFile.ReadHandle, ResultSet, A]

  object ViewState {
    def Ops[S[_]](
      implicit S: ViewState :<: S
    ): KeyValueStore.Ops[ReadFile.ReadHandle, ResultSet, S] =
      KeyValueStore.Ops[ReadFile.ReadHandle, ResultSet, S]

    def toTask(initial: ViewHandles): Task[ViewState ~> Task] =
      TaskRef(initial) map KeyValueStore.fromTaskRef

    def toState[S](l: Lens[S, ViewHandles]): ViewState ~> State[S, ?] =
      KeyValueStore.toState[State[S,?]](l)
  }

  type ViewFileSystem0[A] = Coproduct[MonotonicSeq, FileSystem, A]
  type ViewFileSystem1[A] = Coproduct[ViewState, ViewFileSystem0, A]
  type ViewFileSystem[A]  = Coproduct[MountConfigs, ViewFileSystem1, A]

  def interpretViewFileSystem[M[_]](
    mc: MountConfigs ~> M,
    v: ViewState ~> M,
    s: MonotonicSeq ~> M,
    fs: FileSystem ~> M
  ): ViewFileSystem ~> M =
    mc :+: v :+: s :+: fs
}

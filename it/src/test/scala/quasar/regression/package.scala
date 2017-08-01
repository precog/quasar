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

import slamdata.Predef.{Long, Map}
import quasar.contrib.pathy.ADir
import quasar.effect._
import quasar.fp.TaskRef
import quasar.fp.free, free._
import quasar.fs.{QueryFile, BackendEffect}

import scalaz.{Failure => _, _}
import scalaz.concurrent._
import scalaz.syntax.apply._

package object regression {
  import quasar.fs.mount.hierarchical.MountedResultH

  type BackendEffectIO[A] = Coproduct[Task, BackendEffect, A]

  type HfsIO0[A] = Coproduct[MountedResultH, Task, A]
  type HfsIO[A]  = Coproduct[MonotonicSeq, HfsIO0, A]

  val interpretHfsIO: Task[HfsIO ~> Task] = {
    import QueryFile.ResultHandle
    import quasar.fs.mount.hierarchical._

    def handlesTask(
      ref: TaskRef[Map[ResultHandle, (ADir, ResultHandle)]]
    ): MountedResultH ~> Task =
      KeyValueStore.impl.fromTaskRef(ref)

    def monoSeqTask(ref: TaskRef[Long]): MonotonicSeq ~> Task =
      MonotonicSeq.fromTaskRef(ref)

    (TaskRef(Map.empty[ResultHandle, (ADir, ResultHandle)]) |@| TaskRef(0L))(
      (handles, ct) => monoSeqTask(ct) :+: handlesTask(handles) :+: NaturalTransformation.refl)
  }
}

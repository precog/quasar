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

package quasar

import quasar.Predef.{Long, Map}
import quasar.fs.{QueryFile, FileSystem, ADir}
import quasar.fp.{free, TaskRef}
import quasar.effect._

import scalaz.{Failure => _, _}
import scalaz.syntax.apply._
import scalaz.concurrent._

package object regression {
  import quasar.fs.mount.hierarchical.{HFSFailureF, MountedResultHF}

  type FileSystemIO[A] = Coproduct[Task, FileSystem, A]

  type HfsIO0[A] = Coproduct[MountedResultHF, Task, A]
  type HfsIO1[A] = Coproduct[HFSFailureF, HfsIO0, A]
  type HfsIO[A]  = Coproduct[MonotonicSeqF, HfsIO1, A]

  val interpretHfsIO: Task[HfsIO ~> Task] = {
    import QueryFile.ResultHandle
    import quasar.fs.mount.hierarchical._

    def handlesTask(
      ref: TaskRef[Map[ResultHandle, (ADir, ResultHandle)]]
    ) : MountedResultHF ~> Task =
      Coyoneda.liftTF[MountedResultH, Task](
        KeyValueStore.fromTaskRef(ref))

    def monoSeqTask(ref: TaskRef[Long]): MonotonicSeqF ~> Task =
      Coyoneda.liftTF[MonotonicSeq, Task](
        MonotonicSeq.fromTaskRef(ref))

    val hfsFailTask: HFSFailureF ~> Task =
      Coyoneda.liftTF[HFSFailure, Task](
        Failure.toTaskFailure[HierarchicalFileSystemError])

    (TaskRef(Map.empty[ResultHandle, (ADir, ResultHandle)]) |@| TaskRef(0L))(
      (handles, ct) => free.interpret4(
        monoSeqTask(ct),
        hfsFailTask,
        handlesTask(handles),
        NaturalTransformation.refl))
  }
}

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

package quasar.main

import quasar.fp._
import quasar.fp.free._
import quasar.fs._
import quasar.fs.mount._, BackendDef.DefinitionResult
import quasar.metastore._

import doobie.imports._
import eu.timepit.refined.auto._
import scalaz.{Failure => _, Lens => _, _}
import scalaz.concurrent.Task

object metastore {

  def jdbcMounter[S[_]](
      hfsRef: TaskRef[BackendEffect ~> HierarchicalFsEffM],
      mntdRef: TaskRef[Mounts[DefinitionResult[PhysFsEffM]]]
  )(
      implicit
      S0: ConnectionIO :<: S,
      S1: PhysErr :<: S,
      S2: FsAsk :<: S): Mounting ~> Free[S, ?] = {
    type M[A] = Free[MountEff, A]
    type G[A] = Coproduct[ConnectionIO, M, A]
    type T[A] = Coproduct[Task, S, A]

    val t: T ~> S =
      S0.compose(taskToConnectionIO) :+: reflNT[S]

    val g: G ~> Free[S, ?] =
      injectFT[ConnectionIO, S] :+:
        foldMapNT(mapSNT(t) compose MountEff.interpreter[T](hfsRef, mntdRef))

    def mounter(handler: MountRequestHandler[PhysFsEffM, HierarchicalFsEff]) = {
      MetaStoreMounter[M, G](handler.mount[MountEff](_), handler.unmount[MountEff](_))
    }

    λ[Mounting ~> Free[S, ?]] { mounting =>
      mountHandler[S].flatMap(handler => (foldMapNT(g) compose mounter(handler))(mounting))
    }
  }
}

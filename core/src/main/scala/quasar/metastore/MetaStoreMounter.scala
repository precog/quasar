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

package quasar.metastore

import slamdata.Predef._
import quasar.fp.free.lift
import quasar.fp.ski._
import quasar.fs.mount._
import quasar.contrib.pathy.{ADir, APath}

import doobie.imports._
import scalaz._, Scalaz._

object MetaStoreMounter {
  import MetaStoreAccess._

  /** Interpret `Mounting` into `ConnectionIO`, using the supplied functions
    * to handle mount and unmount requests, and the `Mounts` table as the
    * `PathStore`.
    */
  def apply[F[_], S[_]](
    mount: MountRequest => F[MountingError \/ Unit],
    unmount: MountRequest => F[Unit]
  )(implicit
    S0: F :<: S,
    S1: ConnectionIO :<: S
  ): Mounting ~> Free[S, ?] = {
    Mounter[Free[S, ?]](
      req => EitherT(lift(mount(req)).into[S]),
      req => lift(unmount(req)).into[S],
      new Mounter.PathStore[Free[S, ?], MountConfig] {
        def get(path: APath) =
          EitherT(OptionT(lift(lookupMountConfig(path)).into[S]))
        def descendants(dir: ADir) =
          lift(mountsHavingPrefix(dir).map(_.keys.toSet)).into[S]
        def insert(path: APath, value: MountConfig) =
          lift(insertMount(path, value).attempt
                .flatMap(_.fold(
                  κ(HC.rollback.as(false)),
                  κ(true.point[ConnectionIO])))).into[S]
        def delete(path: APath) =
          lift(deleteMount(path).run.void).into[S]
      })
  }
}

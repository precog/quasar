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

package quasar.fs.mount

import quasar.Predef._
import quasar.effect._
import quasar.fs.{Path => _, _}
import quasar.fp._

import pathy.Path._
import scalaz._, Scalaz._

object Mounter {
  import Mounting._, MountConfig2._

  /** `Mounting` interpreter interpreting into `KeyValueStore`, using the
    * supplied functions to handle mount and unmount requests.
    *
    * @param mount   Called to handle a mount request.
    * @param unmount Called to handle the removal/undoing of a mount request.
    */
  def apply[F[_], S[_]: Functor](
    mount: MountRequest => F[MountingError \/ Unit],
    unmount: MountRequest => F[Unit]
  )(implicit
    S0: F :<: S,
    S1: MountConfigsF :<: S
  ): Mounting ~> Free[S, ?] = {

    type FreeS[A]  = Free[S, A]
    type MntE[A]   = MntErrT[FreeS, A]
    type Err[E, A] = EitherT[FreeS, E, A]

    val mountConfigs = KeyValueStore.Ops[APath, MountConfig2, S]
    val merr = MonadError[Err, MountingError]

    def mount0(req: MountRequest): MntE[Unit] =
      EitherT[FreeS, MountingError, Unit](free.lift(mount(req)).into[S])

    def unmount0(req: MountRequest): FreeS[Unit] =
      free.lift(unmount(req)).into[S]

    def handleRequest(req: MountRequest): MntE[Unit] = {
      val failIfExisting: MntE[Unit] =
        mountConfigs.get(req.path).as(pathExists(req.path)).toLeft(())

      val putOrUnmount: MntE[Unit] =
        mountConfigs.compareAndPut(req.path, None, req.toConfig)
          .liftM[MntErrT].ifM(
            merr.point(()),
            unmount0(req).liftM[MntErrT] <*
              merr.raiseError(pathExists(req.path)))

      failIfExisting *> mount0(req) *> putOrUnmount
    }

    new (Mounting ~> FreeS) {
      def apply[A](ma: Mounting[A]) = ma match {
        case Lookup(path) =>
          mountConfigs.get(path).run

        case MountView(loc, query, vars) =>
          handleRequest(MountRequest.mountView(loc, query, vars)).run

        case MountFileSystem(loc, typ, uri) =>
          handleRequest(MountRequest.mountFileSystem(loc, typ, uri)).run

        case Unmount(path) =>
          mountConfigs.get(path)
            .flatMap(cfg => OptionT(mkMountRequest(path, cfg).point[FreeS]))
            .flatMapF(req => mountConfigs.delete(path) *> unmount0(req))
            .toRight(pathNotFound(path))
            .run
      }
    }
  }

  /** A mounter where all mount requests succeed trivially.
    *
    * Useful in scenarios where only the bookkeeping of mounts is needed.
    */
  def trivial[S[_]: Functor](implicit S: MountConfigsF :<: S): Mounting ~> Free[S, ?] =
    new (Mounting ~> Free[S, ?]) {
      type F[A] = Coproduct[Id, S, A]
      type M[A] = Free[S, A]
      val mnt = Mounter[Id, F](κ(().right), κ(()))
      def apply[A](m: Mounting[A]) =
        mnt(m).foldMap[M](free.interpret2[Id, S, M](pointNT[M], liftFT[S]))
    }

  ////

  private val pathNotFound = MountingError.pathError composePrism PathError2.pathNotFound
  private val pathExists = MountingError.pathError composePrism PathError2.pathExists

  private def mkMountRequest(path: APath, cfg: MountConfig2): Option[MountRequest] =
    refineType(path).fold(
      d => fileSystemConfig.getOption(cfg) map { case (t, u) =>
        MountRequest.mountFileSystem(d, t, u)
      },
      f => viewConfig.getOption(cfg) map { case (q, vs) =>
        MountRequest.mountView(f, q, vs)
      })
}

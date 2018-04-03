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

package quasar.fs.mount

import slamdata.Predef._
import quasar.contrib.pathy._
import quasar.effect._
import quasar.fs._
import quasar.fp.ski._
import quasar.fp._, free._

import pathy.Path, Path._
import scalaz._, Scalaz._

object Mounter {
  import Mounting._, MountConfig._

  /** A kind of key-value store where the keys are absolute paths, with an
    * additional operation to efficiently look up nested paths. */
  trait MountStore[F[_]] {
    /** The current config for a path, if any. */
    def get(path: APath): EitherT[OptionT[F, ?], MountingError, MountConfig]

    /** The type of configs nested within the given path, indexed by path. */
    def descendants(dir: ADir): F[Map[APath, MountingError \/ MountType]]

    /** Associate a value with a path that is not already present, or else do
      * nothing. Yields true if the write occurred. */
    def insert(path: APath, value: MountConfig): F[Boolean]

    /** Remove a path and its value, if present, or do nothing */
    def delete(path: APath): F[Unit]
  }

  /** `Mounting` interpreter */
  def apply[F[_]: Monad](
    mount: MountRequest => MntErrT[F, Unit],
    unmount: MountRequest => F[Unit],
    store: MountStore[F]
  ): Mounting ~> F = {
    type MntE[A] = MntErrT[F, A]

    val merr = MonadError[MntE, MountingError]

    def failIfExisting(path: APath): MntE[Unit] =
      store.get(path).run.as(pathExists(path)).toLeft(())

    def handleMount(req: MountRequest): MntE[Unit] = {
      val putOrUnmount: MntE[Unit] =
        store.insert(req.path, req.toConfig)
          .liftM[MntErrT].ifM(
            merr.point(()),
            unmount(req).liftM[MntErrT] <*
              merr.raiseError(pathExists(req.path)))

      failIfExisting(req.path) *> mount(req) *> putOrUnmount
    }

    def handleUnmount(path: APath): OptionT[F, Unit] =
      store.get(path).run
        .flatMap(i => OptionT((i.toOption >>= (cfg => mkMountRequest(path, cfg))).η[F]))
        .flatMapF(req => store.delete(path) *> unmount(req))

    def handleRemount(src: APath, dst: APath): MntE[Unit] = {
      def reqOrFail(path: APath, cfg: MountConfig): MntE[MountRequest] =
        OptionT(mkMountRequest(path, cfg).point[F])
          .toRight(MountingError.invalidConfig(cfg, "config type mismatch".wrapNel))

      for {
        cfg    <- EitherT(store.get(src).run.run ∘ (i => (i \/> pathNotFound(src)).join))
        srcReq <- reqOrFail(src, cfg)
        dstReq <- reqOrFail(dst, cfg)

        _      <- store.delete(src).liftM[MntErrT]
        _      <- unmount(srcReq).liftM[MntErrT]

        upd    =  store.insert(dst, dstReq.toConfig)
        _      <- OptionT(upd.map(_.option(()))).toRight(pathExists(dst))
        _      <- mount(dstReq)
      } yield ()
    }

    λ[Mounting ~> F] {
      case HavingPrefix(dir) =>
        store.descendants(dir)

      case LookupType(path) =>
        store.get(path).map(MountType.fromConfig).run.run

      case LookupConfig(path) =>
        store.get(path).run.run

      case MountView(loc, query, vars) =>
        handleMount(MountRequest.mountView(loc, query, vars)).run

      case MountFileSystem(loc, typ, uri) =>
        handleMount(MountRequest.mountFileSystem(loc, typ, uri)).run

      case MountModule(loc, statements) =>
        handleMount(MountRequest.mountModule(loc, statements)).run

      case Unmount(path) =>
        handleUnmount(path)
          .flatMapF(_ =>
            refineType(path).swap
              .foldMapM(d => store.descendants(d) map (_.keySet))
              .flatMap(_.traverse_(handleUnmount(_)).run.void))
          .toRight(pathNotFound(path))
          .run

      case Remount(src, dst) =>
        if (src ≟ dst)
          store.get(src).run.void.toRight(pathNotFound(src)).run
        else {
          def move[T](srcDir: ADir, dstDir: ADir, p: Path[Abs,T,Sandboxed]): F[Unit] =
            p.relativeTo(srcDir)
              .map(rel => handleRemount(p, dstDir </> rel).run.void)
              .sequence_

          val moveNested: F[Unit] =
            (maybeDir(src) |@| maybeDir(dst))((srcDir, dstDir) =>
              store.descendants(srcDir).flatMap(_.keySet.traverse_(move(srcDir, dstDir, _)))).sequence_

          // It's important to move the descendants first or else we will move the mount itself again
          // if it's being moved to a path that is below it's current path. i.e. moving /foo/ to /foo/bar/
          failIfExisting(dst) >> moveNested.liftM[MntErrT] >> handleRemount(src, dst)
        }.run
    }
  }

  /** `Mounting` interpreter using `KeyValueStore` to persist the configuration,
    * and the supplied functions to handle mount and unmount requests.
    *
    * @param mount   Called to handle a mount request.
    * @param unmount Called to handle the removal/undoing of a mount request.
    */
  def kvs[F[_], S[_]](
    mount: MountRequest => F[MountingError \/ Unit],
    unmount: MountRequest => F[Unit]
  )(implicit
    S0: F :<: S,
    S1: MountConfigs :<: S
  ): Mounting ~> Free[S, ?] = {
    val mountConfigs = KeyValueStore.Ops[APath, MountConfig, S]

    Mounter[Free[S, ?]](
      req => EitherT[Free[S, ?], MountingError, Unit](free.lift(mount(req)).into[S]),
      req => free.lift(unmount(req)).into[S],
      new MountStore[Free[S, ?]] {
        def get(path: APath) =
          EitherT.rightT(mountConfigs.get(path))

        def descendants(dir: ADir) =
          for {
            paths <- mountConfigs.keys

            descs =  paths.filter(p => (dir: APath) ≠ p && p.relativeTo(dir).isDefined)

            pairs <- descs.traverse { p =>
              mountConfigs.get(p).map(MountType.fromConfig andThen ((p, _))).run
            }
          } yield pairs.unite.toMap.mapValues(_.right[MountingError])

        def insert(path: APath, value: MountConfig) =
          mountConfigs.compareAndPut(path, None, value)

        def delete(path: APath) =
          mountConfigs.delete(path)
      })
  }

  /** A mounter where all mount requests succeed trivially.
    *
    * Useful in scenarios where only the bookkeeping of mounts is needed.
    */
  def trivial[S[_]](implicit S: MountConfigs :<: S): Mounting ~> Free[S, ?] = {
    type F[A] = Coproduct[Id, S, A]
    val mnt   = Mounter.kvs[Id, F](κ(().right), κ(()))

    λ[Mounting ~> Free[S, ?]](m => mnt(m) foldMap (pointNT[Free[S, ?]] :+: liftFT[S]))
  }

  private val pathNotFound = MountingError.pathError composePrism PathError.pathNotFound
  private val pathExists   = MountingError.pathError composePrism PathError.pathExists

  private def mkMountRequest(path: APath, cfg: MountConfig): Option[MountRequest] =
    refineType(path).fold(
      d => fileSystemConfig.getOption(cfg) map { case (t, u) =>
        MountRequest.mountFileSystem(d, t, u)
      } orElse {
        moduleConfig.getOption(cfg) map (MountRequest.mountModule(d, _))
      },
      f => viewConfig.getOption(cfg) map { case (q, vs) =>
        MountRequest.mountView(f, q, vs)
      })
}

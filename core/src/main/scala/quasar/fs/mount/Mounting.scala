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
import quasar.Variables
import quasar.contrib.pathy._
import quasar.effect.LiftedOps
import quasar.fp._
import quasar.fs._
import quasar.sql.Sql

import matryoshka.Fix
import monocle.Prism
import monocle.std.{disjunction => D}
import pathy._, Path._
import scalaz._, Scalaz._

sealed trait Mounting[A]

object Mounting {

  /** Provides for accessing many mounts at once, which allows certain
    * operations (like determining which paths in a directory refer to
    * mounts) to be implemented more efficiently.
    */
  final case class HavingPrefix(dir: ADir)
    extends Mounting[Map[APath, MountType]]

  final case class LookupType(path: APath)
    extends Mounting[Option[MountType]]

  final case class LookupConfig(path: APath)
    extends Mounting[Option[MountConfig]]

  final case class MountView(loc: AFile, query: Fix[Sql], vars: Variables)
    extends Mounting[MountingError \/ Unit]

  final case class MountFileSystem(loc: ADir, typ: FileSystemType, uri: ConnectionUri)
    extends Mounting[MountingError \/ Unit]

  final case class Unmount(path: APath)
    extends Mounting[MountingError \/ Unit]

  /** Indicates the wrong type of path (file vs. dir) was supplied to the `mount`
    * convenience function.
    */
  final case class PathTypeMismatch(path: APath) extends scala.AnyVal

  object PathTypeMismatch {
    implicit val pathTypeMismatchShow: Show[PathTypeMismatch] =
      Show.shows { v =>
        val expectedType = refineType(v.path).fold(κ("file"), κ("directory"))
        s"Expected ${expectedType} path instead of '${posixCodec.printPath(v.path)}'"
      }
  }

  final class Ops[S[_]](implicit S: Mounting :<: S)
    extends LiftedOps[Mounting, S] {

    import MountConfig._

    /** Returns mounts located at a path having the given prefix. */
    def havingPrefix(dir: ADir): F[Map[APath, MountType]] =
      lift(HavingPrefix(dir))

    /** The views mounted at paths having the given prefix. */
    def viewsHavingPrefix(dir: ADir): F[Set[AFile]] =
      rPathsHavingPrefix(dir).map(_.foldMap(_.toSet))

    /** The filesystems mounted at paths having the given prefix. */
    def fsHavingPrefix(dir: ADir): F[Set[ADir]] =
      rPathsHavingPrefix(dir).map(_.foldMap(_.swap.toSet))

    /** Whether the given path refers to a mount. */
    def exists(path: APath): F[Boolean] =
      lookupType(path).isDefined

    /** Returns the mount configuration if the given path refers to a mount. */
    def lookupConfig(path: APath): OptionT[F, MountConfig] =
      OptionT(lift(LookupConfig(path)))

    /** Returns the type of mount the path refers to, if any. */
    def lookupType(path: APath): OptionT[F, MountType] =
      OptionT(lift(LookupType(path)))

    /** Create a view mount at the given location. */
    def mountView(
      loc: AFile,
      query: Fix[Sql],
      vars: Variables
    )(implicit
      S0: MountingFailure :<: S
    ): F[Unit] =
      MountingFailure.Ops[S].unattempt(lift(MountView(loc, query, vars)))

    /** Create a filesystem mount at the given location. */
    def mountFileSystem(
      loc: ADir,
      typ: FileSystemType,
      uri: ConnectionUri
    )(implicit
      S0: MountingFailure :<: S
    ): F[Unit] =
      MountingFailure.Ops[S].unattempt(lift(MountFileSystem(loc, typ, uri)))

    /** Attempt to create a mount described by the given configuration at the
      * given location.
      */
    def mount(
      loc: APath,
      config: MountConfig
    )(implicit
      S0: MountingFailure :<: S,
      S1: PathMismatchFailure :<: S
    ): F[Unit] = {
      val mmErr = PathMismatchFailure.Ops[S]

      config match {
        case ViewConfig(query, vars) =>
          D.right.getOption(refineType(loc)) cata (
            file => mountView(file, query, vars),
            mmErr.fail(PathTypeMismatch(loc)))

        case FileSystemConfig(typ, uri) =>
          D.left.getOption(refineType(loc)) cata (
            dir => mountFileSystem(dir, typ, uri),
            mmErr.fail(PathTypeMismatch(loc)))
      }
    }

    /** Remount `src` at `dst`, results in an error if there is no mount at
      * `src`.
      */
    def remount[T](
      src: Path[Abs,T,Sandboxed],
      dst: Path[Abs,T,Sandboxed]
    )(implicit
      S0: MountingFailure :<: S,
      S1: PathMismatchFailure :<: S
    ): F[Unit] =
      modify(src, dst, ι).void

    /** Replace the mount at the given path with one described by the
      * provided config.
      */
    def replace(
      loc: APath,
      config: MountConfig
    )(implicit
      S0: MountingFailure :<: S,
      S1: PathMismatchFailure :<: S
    ): F[Unit] =
      modify(loc, loc, κ(config))

    /** Remove the mount at the given path. */
    def unmount(path: APath)(implicit S0: MountingFailure :<: S): F[Unit] =
      MountingFailure.Ops[S].unattempt(lift(Unmount(path)))

    ////

    private val notFound: Prism[MountingError, APath] =
      MountingError.pathError composePrism PathError.pathNotFound

    private def modify[T](
      src: Path[Abs,T,Sandboxed],
      dst: Path[Abs,T,Sandboxed],
      f: MountConfig => MountConfig
    )(implicit
      S0: MountingFailure :<: S,
      S1: PathMismatchFailure :<: S
    ): F[Unit] = {
      val mntErr = MountingFailure.Ops[S]
      val mmErr = PathMismatchFailure.Ops[S]

      for {
        cfg     <- lookupConfig(src) getOrElseF mntErr.fail(notFound(src))
        _       <- unmount(src)
        mod     =  mount(dst, f(cfg))
        restore =  mount(src, cfg)
        res1    =  mntErr.onFail(mod, err => restore *> mntErr.fail(err))
        _       <- mmErr.onFail(res1, err => restore *> mmErr.fail(err))
      } yield ()
    }

    private def rPathsHavingPrefix(dir: ADir): F[Set[ADir \/ AFile]] =
      havingPrefix(dir).map(_.keySet.map(refineType))
  }

  object Ops {
    implicit def apply[S[_]](implicit S: Mounting :<: S): Ops[S] =
      new Ops[S]
  }
}

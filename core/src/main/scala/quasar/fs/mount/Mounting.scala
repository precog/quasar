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

package quasar.fs.mount

import slamdata.Predef._
import quasar.Variables
import quasar.contrib.pathy._
import quasar.effect.LiftedOps
import quasar.fp.ski._
import quasar.fs._
import quasar.sql.{ScopedExpr, Sql, Statement}

import matryoshka.data.Fix
import monocle.Prism
import monocle.std.{disjunction => D}
import pathy._, Path._
import scalaz._, Scalaz._

sealed abstract class Mounting[A]

object Mounting {

  /** Provides for accessing many mounts at once, which allows certain
    * operations (like determining which paths in a directory refer to
    * mounts) to be implemented more efficiently.
    */
  final case class HavingPrefix(dir: ADir)
    extends Mounting[Map[APath, MountingError \/ MountType]]

  final case class LookupType(path: APath)
      extends Mounting[Option[MountingError \/ MountType]]

  final case class LookupConfig(path: APath)
    extends Mounting[Option[MountingError \/ MountConfig]]

  final case class MountView(loc: AFile, scopedExpr: ScopedExpr[Fix[Sql]], vars: Variables)
    extends Mounting[MountingError \/ Unit]

  final case class MountFileSystem(loc: ADir, typ: FileSystemType, uri: ConnectionUri)
    extends Mounting[MountingError \/ Unit]

  final case class MountModule(loc: ADir, statements: List[Statement[Fix[Sql]]])
    extends Mounting[MountingError \/ Unit]

  final case class Unmount(path: APath)
    extends Mounting[MountingError \/ Unit]

  final case class Remount[T](from: Path[Abs,T,Sandboxed], to: Path[Abs,T,Sandboxed])
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
    def havingPrefix(dir: ADir): FreeS[Map[APath, MountingError \/ MountType]] =
      lift(HavingPrefix(dir))

    /** The views mounted at paths having the given prefix. */
    def viewsHavingPrefix(dir: ADir): FreeS[Set[AFile]] =
      rPathsHavingPrefix(dir).map(_.foldMap(_.toSet))

    def viewsHavingPrefix_(dir: ADir): FreeS[Set[RFile]] =
      viewsHavingPrefix(dir).map(_.foldMap(_.relativeTo(dir).toSet))

    def modulesHavingPrefix(dir: ADir): FreeS[Set[ADir]] =
      havingPrefix(dir).map(_.collect {
        case (k, \/-(v)) if v ≟ MountType.ModuleMount => k }.toSet
        .foldMap(p => refineType(p).swap.toSet))

    def modulesHavingPrefix_(dir: ADir): FreeS[Set[RDir]] =
      modulesHavingPrefix(dir).map(_.foldMap(_.relativeTo(dir).toSet))

    /** Whether the given path refers to a mount. */
    def exists(path: APath): FreeS[Boolean] =
      lookupType(path).run.isDefined

    /** Returns the mount configuration if the given path refers to a mount. */
    def lookupConfig(path: APath): EitherT[OptionT[FreeS, ?], MountingError, MountConfig] =
      EitherT(OptionT(lift(LookupConfig(path))))

    def lookupViewConfig(path: AFile): EitherT[OptionT[FreeS, ?], MountingError, ViewConfig] =
      lookupConfig(path).flatMap(config =>
        EitherT.right(OptionT(viewConfig.getOption(config).map(ViewConfig.tupled).point[FreeS])))

    def lookupModuleConfig(path: ADir): EitherT[OptionT[FreeS, ?], MountingError, ModuleConfig] =
      lookupConfig(path).flatMap(config =>
        EitherT.right(OptionT(moduleConfig.getOption(config).map(ModuleConfig(_)).point[FreeS])))

    /** Returns the type of mount the path refers to, if any. */
    def lookupType(path: APath): EitherT[OptionT[FreeS, ?], MountingError, MountType] =
      EitherT(OptionT(lift(LookupType(path))))

    /** Create a view mount at the given location. */
    def mountView(
      loc: AFile,
      scopedExpr: ScopedExpr[Fix[Sql]],
      vars: Variables
    )(implicit
      S0: MountingFailure :<: S
    ): FreeS[Unit] =
      MountingFailure.Ops[S].unattempt(lift(MountView(loc, scopedExpr, vars)))

    /** Create a filesystem mount at the given location. */
    def mountFileSystem(
      loc: ADir,
      typ: FileSystemType,
      uri: ConnectionUri
    )(implicit
      S0: MountingFailure :<: S
    ): FreeS[Unit] =
      MountingFailure.Ops[S].unattempt(lift(MountFileSystem(loc, typ, uri)))

    def mountModule(
      loc: ADir,
      statements: List[Statement[Fix[Sql]]]
    )(implicit
      SO: MountingFailure :<: S
    ): FreeS[Unit] =
      MountingFailure.Ops[S].unattempt(lift(MountModule(loc, statements)))

    /** Attempt to create a mount described by the given configuration at the
      * given location.
      */
    def mount(
      loc: APath,
      config: MountConfig
    )(implicit
      S0: MountingFailure :<: S,
      S1: PathMismatchFailure :<: S
    ): FreeS[Unit] = {
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

        case ModuleConfig(statements) =>
          D.left.getOption(refineType(loc)) cata (
            dir => mountModule(dir, statements),
            mmErr.fail(PathTypeMismatch(loc)))
      }
    }

    /** Replace the mount at the given path with one described by the
      * provided config.
      */
    def replace(
      loc: APath,
      config: MountConfig
    )(implicit
      S0: MountingFailure :<: S,
      S1: PathMismatchFailure :<: S
    ): FreeS[Unit] =
      modify(loc, loc, κ(config))

    /** Remove the mount at the given path. */
    def unmount(path: APath)(implicit S0: MountingFailure :<: S): FreeS[Unit] =
      MountingFailure.Ops[S].unattempt(lift(Unmount(path)))

    /** Remount `src` at `dst`, results in an error if there is no mount at
      * `src`.
      */
    def remount[T](
      src: Path[Abs,T,Sandboxed],
      dst: Path[Abs,T,Sandboxed]
    )(implicit S0: MountingFailure :<: S): FreeS[Unit] =
      MountingFailure.Ops[S].unattempt(lift(Remount(src, dst)))

    def mountOrReplace(
      path: APath,
      mountConfig: MountConfig,
      replaceIfExists: Boolean
    )(implicit
      S0: MountingFailure :<: S,
      S1: PathMismatchFailure :<: S
    ): Free[S, Unit] =
      for {
        exists <- lookupType(path).run.isDefined
        _      <- if (replaceIfExists && exists) replace(path, mountConfig)
                  else mount(path, mountConfig)
      } yield ()

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
    ): FreeS[Unit] = {
      val mntErr = MountingFailure.Ops[S]
      val mmErr = PathMismatchFailure.Ops[S]

      for {
        cfg     <- lookupConfig(src).run.run >>=[MountConfig] (
                     _.cata(_.fold(mntErr.fail(_), _.η[FreeS]), mntErr.fail(notFound(src))))
        _       <- unmount(src)
        mod     =  mount(dst, f(cfg))
        restore =  mount(src, cfg)
        res1    =  mntErr.onFail(mod, err => restore *> mntErr.fail(err))
        _       <- mmErr.onFail(res1, err => restore *> mmErr.fail(err))
      } yield ()
    }

    private def rPathsHavingPrefix(dir: ADir): FreeS[Set[ADir \/ AFile]] =
      havingPrefix(dir).map(_.keySet.map(refineType))
  }

  object Ops {
    implicit def apply[S[_]](implicit S: Mounting :<: S): Ops[S] =
      new Ops[S]
  }
}

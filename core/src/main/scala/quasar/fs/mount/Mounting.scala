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
import quasar.effect.LiftedOps
import quasar.fp._
import quasar.fs.{Path => _, _}
import quasar.sql._

import monocle.Prism
import monocle.std.{disjunction => D}
import pathy._, Path._
import scalaz._, Scalaz._

sealed trait Mounting[A]

object Mounting {
  final case class Lookup(path: APath)
    extends Mounting[Option[MountConfig2]]

  final case class MountView(loc: AFile, query: Expr, vars: Variables)
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

  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.NonUnitStatements"))
  final class Ops[S[_]](implicit S0: Functor[S], S1: MountingF :<: S)
    extends LiftedOps[Mounting, S] {

    import MountConfig2._

    type M[A] = EitherT[F, MountingError, A]

    /** Returns the mount configuration for the given mount path or nothing
      * if the path does not refer to a mount.
      */
    def lookup(path: APath): OptionT[F, MountConfig2] =
      OptionT(lift(Lookup(path)))

    /** Create a view mount at the given location. */
    def mountView(loc: AFile, query: Expr, vars: Variables): M[Unit] =
      EitherT(lift(MountView(loc, query, vars)))

    /** Create a filesystem mount at the given location. */
    def mountFileSystem(loc: ADir, typ: FileSystemType, uri: ConnectionUri): M[Unit] =
      EitherT(lift(MountFileSystem(loc, typ, uri)))

    /** Attempt to create a mount described by the given configuration at the
      * given location.
      */
    def mount(loc: APath, config: MountConfig2): M[PathTypeMismatch \/ Unit] =
      config match {
        case ViewConfig(query, vars) =>
          D.right.getOption(refineType(loc)) cata (
            file => mountView(file, query, vars).map(_.right),
            PathTypeMismatch(loc).left.point[M])

        case FileSystemConfig(typ, uri) =>
          D.left.getOption(refineType(loc)) cata (
            dir => mountFileSystem(dir, typ, uri).map(_.right),
            PathTypeMismatch(loc).left.point[M])
      }

    /** Remount `src` at `dst`, results in an error if there is no mount at
      * `src`.
      */
    def remount[T](src: Path[Abs,T,Sandboxed], dst: Path[Abs,T,Sandboxed]): M[Unit] =
      modify(src, dst, ι).void

    /** Replace the mount at the given path with one described by the
      * provided config.
      */
    def replace(loc: APath, config: MountConfig2): M[PathTypeMismatch \/ Unit] =
      modify(loc, loc, κ(config))

    /** Remove the mount at the given path. */
    def unmount(path: APath): M[Unit] =
      EitherT(lift(Unmount(path)))

    ////

    private type ErrF[A, B] = EitherT[F, A, B]

    private def bifold[G[_]: Functor, E, A, EE, AA](v: EitherT[G,E,A])(e: E => EE \/ AA, a: A => EE \/ AA): EitherT[G, EE, AA] =
      EitherT[G,EE,AA](v.run.map(_.fold(e, a)))

    private def toLeft[G[_]: Functor, E1, E2, A](v: EitherT[G, E1, E2 \/ A]): EitherT[G, E1 \/ E2, A] =
      bifold(v)(_.left.left, _.fold(_.right.left, _.right))

    private def toRight[G[_]: Functor, E1, E2, A](v: EitherT[G, E1 \/ E2, A]): EitherT[G, E1, E2 \/ A] =
      bifold(v)(_.fold(_.left, _.left.right), _.right.right)

    private val notFound: Prism[MountingError, APath] =
      MountingError.pathError composePrism PathError2.pathNotFound

    private val invalidPath: Prism[MountingError, (APath, String)] =
      MountingError.pathError composePrism PathError2.invalidPath

    private def modify[T](
      src: Path[Abs,T,Sandboxed],
      dst: Path[Abs,T,Sandboxed],
      f: MountConfig2 => MountConfig2
    ): M[PathTypeMismatch \/ Unit] = {
      val mErr = MonadError[ErrF, MountingError \/ PathTypeMismatch]
      import mErr._

      for {
        cfg <- lookup(src) toRight notFound(src)
        _   <- unmount(src)
        rez <- toRight(handleError(toLeft(mount(dst, f(cfg)))) { err =>
                 toLeft(mount(src, cfg)) *> raiseError(err)
               })
      } yield rez
    }
  }

  object Ops {
    implicit def apply[S[_]](implicit S0: Functor[S], S1: MountingF :<: S): Ops[S] =
      new Ops[S]
  }
}

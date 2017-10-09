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

package quasar.fs

import slamdata.Predef._
import quasar._, RenderTree.ops._
import quasar.contrib.pathy._
import quasar.effect.LiftedOps
import quasar.fp._
import quasar.fp.ski._

import monocle.Prism
import pathy.{Path => PPath}, PPath._
import scalaz._, Scalaz._

sealed abstract class ManageFile[A]

object ManageFile {
  sealed abstract class PathPair {
    import PathPair._

    def fold[X](
      d2d: (ADir, ADir) => X,
      f2f: (AFile, AFile) => X
    ): X =
      this match {
        case DirToDir(sd, dd)   => d2d(sd, dd)
        case FileToFile(sf, df) => f2f(sf, df)
      }

    def src: APath

    def dst: APath
  }

  object PathPair {
    final case class DirToDir private (src: ADir, dst: ADir)
        extends PathPair
    final case class FileToFile private (src: AFile, dst: AFile)
        extends PathPair

    val dirToDir: Prism[PathPair, (ADir, ADir)] =
      Prism((_: PathPair).fold((s, d) => (s, d).some, κ2(none)))(DirToDir.tupled)

    val fileToFile: Prism[PathPair, (AFile, AFile)] =
      Prism((_: PathPair).fold(κ2(none), (s, d) => (s, d).some))(FileToFile.tupled)
  }

  final case class Move(pair: PathPair, semantics: MoveSemantics)
    extends ManageFile[FileSystemError \/ Unit]

  final case class Copy(pair: PathPair) extends ManageFile[FileSystemError \/ Unit]

  final case class Delete(path: APath)
    extends ManageFile[FileSystemError \/ Unit]

  final case class TempFile(near: APath)
    extends ManageFile[FileSystemError \/ AFile]

  final class Ops[S[_]](implicit S: ManageFile :<: S)
    extends LiftedOps[ManageFile, S] {

    type M[A] = FileSystemErrT[FreeS, A]

    /** Request the given move scenario be applied to the file system, using the
      * given semantics.
      */
    def move(pair: PathPair, semantics: MoveSemantics): M[Unit] =
      EitherT(lift(Move(pair, semantics)))

    /** Move the `src` dir to `dst` dir, requesting the semantics described by `sem`. */
    def moveDir(src: ADir, dst: ADir, sem: MoveSemantics): M[Unit] =
      move(PathPair.dirToDir(src, dst), sem)

    /** Move the `src` file to `dst` file, requesting the semantics described by `sem`. */
    def moveFile(src: AFile, dst: AFile, sem: MoveSemantics): M[Unit] =
      move(PathPair.fileToFile(src, dst), sem)

    def copy(pair: PathPair): M[Unit] = EitherT(lift(Copy(pair)))

    def copyDir(src: ADir, dst: ADir): M[Unit] = copy(PathPair.dirToDir(src, dst))

    def copyFile(src: AFile, dst: AFile): M[Unit] = copy(PathPair.fileToFile(src, dst))

    /** Rename the `src` file in the same directory. */
    def renameFile(src: AFile, name: String): M[AFile] = {
      val dst = PPath.renameFile(src, κ(FileName(name)))
      moveFile(src, dst, MoveSemantics.Overwrite).as(dst)
    }

    /** Delete the given file system path, fails if the path does not exist. */
    def delete(path: APath): M[Unit] =
      EitherT(lift(Delete(path)))

    /** Returns the path to a new temporary file as physically close to the
      * supplied path as possible.
      */
    def tempFile(near: APath): M[AFile] =
      EitherT(lift(TempFile(near)))
  }

  object Ops {
    implicit def apply[S[_]](implicit S: ManageFile :<: S): Ops[S] =
      new Ops[S]
  }

  implicit def renderTree[A]: RenderTree[ManageFile[A]] =
    new RenderTree[ManageFile[A]] {
      def render(mf: ManageFile[A]) = mf match {
        case Move(scenario, semantics) => NonTerminal(List("Move"), semantics.shows.some,
          scenario.fold(
            (from, to) => List(from.render, to.render),
            (from, to) => List(from.render, to.render)))
        case Copy(pair) => NonTerminal(List("Copy"), None,
          List(pair.src.render, pair.dst.render))
        case Delete(path) => NonTerminal(List("Delete"), None, List(path.render))
        case TempFile(nearTo) => NonTerminal(List("TempFile"), None, List(nearTo.render))
      }
    }
}

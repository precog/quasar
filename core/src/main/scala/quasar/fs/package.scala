/*
 * Copyright 2014 - 2015 SlamData Inc.
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

import quasar.Predef._
import quasar.effect.FailureF
import quasar.fp.free._

import pathy.{Path => PPath}, PPath._
import scalaz._, Scalaz._

package object fs {
  type ReadFileF[A]    = Coyoneda[ReadFile, A]
  type WriteFileF[A]   = Coyoneda[WriteFile, A]
  type ManageFileF[A]  = Coyoneda[ManageFile, A]
  type QueryFileF[A]   = Coyoneda[QueryFile, A]

  type FileSystem0[A] = Coproduct[WriteFileF, ManageFileF, A]
  type FileSystem1[A] = Coproduct[ReadFileF, FileSystem0, A]
  /** FileSystem[A] = QueryFileF or ReadFileF or WriteFileF or ManageFileF */
  type FileSystem[A]  = Coproduct[QueryFileF, FileSystem1, A]

  type AbsPath[T] = pathy.Path[Abs,T,Sandboxed]
  type RelPath[T] = pathy.Path[Rel,T,Sandboxed]

  type ADir  = AbsDir[Sandboxed]
  type RDir  = RelDir[Sandboxed]
  type AFile = AbsFile[Sandboxed]
  type RFile = RelFile[Sandboxed]
  type APath = AbsPath[_]
  type RPath = RelPath[_]

  type PathName = DirName \/ FileName

  type PathErr2T[F[_], A] = EitherT[F, PathError2, A]
  type FileSystemFailure[A] = FailureF[FileSystemError, A]
  type FileSystemErrT[F[_], A] = EitherT[F, FileSystemError, A]

  def interpretFileSystem[M[_]: Functor](
    q: QueryFile ~> M,
    r: ReadFile ~> M,
    w: WriteFile ~> M,
    m: ManageFile ~> M
  ): FileSystem ~> M =
    interpret4[QueryFileF, ReadFileF, WriteFileF, ManageFileF, M](
      Coyoneda.liftTF(q), Coyoneda.liftTF(r), Coyoneda.liftTF(w), Coyoneda.liftTF(m))

  /** Rebases absolute paths onto the provided absolute directory, so
    * `rebaseA(/baz)(/foo/bar)` becomes `/baz/foo/bar`.
    */
  def rebaseA(onto: ADir): AbsPath ~> AbsPath =
    new (AbsPath ~> AbsPath) {
      def apply[T](apath: AbsPath[T]) =
        apath.relativeTo(rootDir[Sandboxed]).fold(apath)(onto </> _)
    }

  /** Removes the given prefix from an absolute path, if present. */
  def stripPrefixA(prefix: ADir): AbsPath ~> AbsPath =
    new (AbsPath ~> AbsPath) {
      def apply[T](apath: AbsPath[T]) =
        apath.relativeTo(prefix).fold(apath)(rootDir </> _)
    }

  /** Returns the first named segment of the given relative path. */
  def firstSegmentName(f: RPath): Option[PathName] =
    flatten(none, none, none,
      n => DirName(n).left.some,
      n => FileName(n).right.some,
      f).toIList.unite.headOption

  /** Sandboxes an absolute path, needed due to parsing functions producing
    * unsandboxed paths.
    *
    * TODO: We know this can't fail, remove once Pathy is refactored to be more precise
    */
  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.OptionPartial"))
  def sandboxAbs[T, S](apath: PPath[Abs,T,S]): PPath[Abs,T,Sandboxed] =
    rootDir[Sandboxed] </> apath.relativeTo(rootDir).get
}

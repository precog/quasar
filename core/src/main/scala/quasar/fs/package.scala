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

import quasar.Predef._
import quasar.effect.Failure
import quasar.fp._
import quasar.fp.free._

import pathy.Path, Path._
import scalaz.{Failure => _, _}, Scalaz._

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

  type FilePath[B] = pathy.Path[B,File,Sandboxed]
  type DirPath[B]  = pathy.Path[B,Dir, Sandboxed]
  type FPath = FilePath[_]
  type DPath = DirPath[_]

  type PathSegment = DirName \/ FileName

  type FileSystemFailure[A] = Failure[FileSystemError, A]
  type FileSystemFailureF[A] = Coyoneda[FileSystemFailure, A]
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
  def firstSegmentName(f: RPath): Option[PathSegment] =
    flatten(none, none, none,
      n => DirName(n).left.some,
      n => FileName(n).right.some,
      f).toIList.unite.headOption

  def prettyPrint(path: Path[_,_,_]): String =
    refineType(path).fold(
      dir => posixCodec.unsafePrintPath(dir),
      file => refineTypeAbs(file).fold(
        abs => posixCodec.unsafePrintPath(abs),
        // Remove the `./` from the beginning of the string representation of a relative path
        rel => posixCodec.unsafePrintPath(rel).drop(2)))

  /** Sandboxes an absolute path, needed due to parsing functions producing
    * unsandboxed paths.
    *
    * TODO[pathy]: We know this can't fail, remove once Pathy is refactored to be more precise
    */
  @SuppressWarnings(Array("org.brianmckenna.wartremover.warts.OptionPartial"))
  def sandboxAbs[T, S](apath: Path[Abs,T,S]): Path[Abs,T,Sandboxed] =
    rootDir[Sandboxed] </> apath.relativeTo(rootDir).get

  // TODO[pathy]: Offer clean API in pathy to do this
  def sandboxCurrent[A,T](path: Path[A,T,Unsandboxed]): Option[Path[A,T,Sandboxed]] =
    refineTypeAbs(path).fold(
      abs => (abs relativeTo rootDir).map(p => (rootDir[Sandboxed] </> p).asInstanceOf[Path[A,T,Sandboxed]]),
      rel => (rel relativeTo currentDir).map(p => (currentDir[Sandboxed] </> p).asInstanceOf[Path[A,T,Sandboxed]]))

  // TODO[pathy]: Offer clean API in pathy to do this
  def refineTypeAbs[T,S](path: Path[_,T,S]): Path[Abs,T,S] \/ Path[Rel,T,S] = {
    if (path.isAbsolute) path.asInstanceOf[Path[Abs,T,S]].left
    else path.asInstanceOf[Path[Rel,T,S]].right
  }

  def mkAbsolute[T,S](baseDir: AbsDir[S], path: Path[_,T,S]): Path[Abs,T,S] =
    refineTypeAbs(path).fold(ι, baseDir </> _)
}

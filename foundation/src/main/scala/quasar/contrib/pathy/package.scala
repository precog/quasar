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

package quasar.contrib

import quasar.Predef._
import quasar.fp.ski._

import argonaut._
import _root_.pathy.Path, Path._
import _root_.scalaz._, Scalaz._

package object pathy {
  type AbsPath[T] = Path[Abs,T,Sandboxed]
  type RelPath[T] = Path[Rel,T,Sandboxed]

  type ADir  = AbsDir[Sandboxed]
  type RDir  = RelDir[Sandboxed]
  type AFile = AbsFile[Sandboxed]
  type RFile = RelFile[Sandboxed]
  type APath = AbsPath[scala.Any]
  type RPath = RelPath[scala.Any]
  type FPath = Path[scala.Any,File,Sandboxed]

  type PathSegment = DirName \/ FileName

  implicit def liftDirName(x: DirName): PathSegment = x.left
  implicit def liftFileName(x: FileName): PathSegment = x.right

  def pathName(p: APath): Option[PathSegment] =
    refineType(p).fold(x => dirName(x) map liftDirName, x => some(fileName(x)))

  implicit val DirNameOrder: Order[DirName] = Order.orderBy(_.value)
  implicit val FileNameOrder: Order[FileName] = Order.orderBy(_.value)

  object APath {

    implicit val aPathDecodeJson: DecodeJson[APath] =
      DecodeJson.of[String] flatMap (s => DecodeJson(hc =>
        posixCodec.parseAbsFile(s).orElse(posixCodec.parseAbsDir(s))
          .map(sandboxAbs)
          .fold(DecodeResult.fail[APath]("[T]AbsPath[T]", hc.history))(DecodeResult.ok)))

  }

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

  /** Returns the first named segment of the given path. */
  def firstSegmentName(p: Path[_,_,_]): Option[PathSegment] =
    flatten(none, none, none,
      n => DirName(n).left.some,
      n => FileName(n).right.some,
      p).toIList.unite.headOption

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
  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  def sandboxAbs[T, S](apath: Path[Abs,T,S]): Path[Abs,T,Sandboxed] =
    rootDir[Sandboxed] </> apath.relativeTo(rootDir).get

  // TODO[pathy]: Offer clean API in pathy to do this
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def sandboxCurrent[A,T](path: Path[A,T,Unsandboxed]): Option[Path[A,T,Sandboxed]] =
    refineTypeAbs(path).fold(
      abs => (abs relativeTo rootDir).map(p => (rootDir[Sandboxed] </> p).asInstanceOf[Path[A,T,Sandboxed]]),
      rel => (rel relativeTo currentDir).map(p => (currentDir[Sandboxed] </> p).asInstanceOf[Path[A,T,Sandboxed]]))

  // TODO[pathy]: Offer clean API in pathy to do this
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def refineTypeAbs[T,S](path: Path[_,T,S]): Path[Abs,T,S] \/ Path[Rel,T,S] = {
    if (path.isAbsolute) path.asInstanceOf[Path[Abs,T,S]].left
    else path.asInstanceOf[Path[Rel,T,S]].right
  }

  def mkAbsolute[T,S](baseDir: AbsDir[S], path: Path[_,T,S]): Path[Abs,T,S] =
    refineTypeAbs(path).fold(ι, baseDir </> _)
}

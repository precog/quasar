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

package quasar.contrib

import slamdata.Predef._
import quasar.fp.ski._

import java.net.{URLDecoder, URLEncoder}

import argonaut._
import _root_.pathy.Path, Path._
import _root_.pathy.argonaut._
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
  type DPath = Path[scala.Any,Dir, Sandboxed]

  type PathSegment = DirName \/ FileName

  implicit def liftDirName(x: DirName): PathSegment = x.left
  implicit def liftFileName(x: FileName): PathSegment = x.right

  def stringValue(seg: PathSegment) = seg.fold(_.value, _.value)

  def pathName(p: APath): Option[PathSegment] =
    refineType(p).fold(x => dirName(x) map liftDirName, x => some(fileName(x)))

  object APath {
    import PosixCodecJson._

    implicit val aPathDecodeJson: DecodeJson[APath] =
      (absDirDecodeJson.widen[APath] ||| absFileDecodeJson).setName("APath")

    implicit val aPathEncodeJson: EncodeJson[APath] =
      pathEncodeJson
  }

  object RPath {
    import PosixCodecJson._

    // this will be sound so long as we're round-tripping
    @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
    private def resandbox[R, T](path: Path[R, T, Unsandboxed]): Path[Rel, T, Sandboxed] =
      Path.sandbox(Path.currentDir, path).get

    implicit val rPathDecodeJson: DecodeJson[RPath] =
      (relDirDecodeJson.map(resandbox(_)).widen[RPath] ||| relFileDecodeJson.map(resandbox(_))).setName("RPath")

    implicit val rPathEncodeJson: EncodeJson[RPath] =
      pathEncodeJson
  }

  /** PathCodec with URI-encoded segments. */
  val UriPathCodec: PathCodec = {
    /** This encoder translates spaces into pluses, but we want the
      * more rigorous %20 encoding.
      */
    val uriEncodeUtf8: String => String = URLEncoder.encode(_, "UTF-8").replace("+", "%20")
    val uriDecodeUtf8: String => String = URLDecoder.decode(_, "UTF-8")

    val escapeRel: String => String = {
      case ".." => "%2E%2E"
      case "."  => "%2E"
      case s    => uriEncodeUtf8(s)
    }

    PathCodec('/', escapeRel, uriDecodeUtf8)
  }

  /** Rebases absolute paths onto the provided absolute directory, so
    * `rebaseA(/baz)(/foo/bar)` becomes `/baz/foo/bar`.
    */
  def rebaseA(onto: ADir): AbsPath ~> AbsPath =
    new (AbsPath ~> AbsPath) {
      def apply[T](apath: AbsPath[T]) =
        apath.relativeTo(root).fold(apath)(onto </> _)
    }

  /** Removes the given prefix from an absolute path, if present. */
  def stripPrefixA(prefix: ADir): AbsPath ~> AbsPath =
    new (AbsPath ~> AbsPath) {
      def apply[T](apath: AbsPath[T]) =
        apath.relativeTo(prefix).fold(apath)(root </> _)
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

  /** This is completely unsafe and should be phased out.
    * Sandboxing is meant to prevent ending up with paths such as `/foo/../../..` and by
    * calling get on the `Option` we are wishful thinking this problem away
    */
  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  def unsafeSandboxAbs[T, S](apath: Path[Abs,T,S]): Path[Abs,T,Sandboxed] =
    root </> apath.relativeTo(root).get

  // TODO[pathy]: Offer clean API in pathy to do this
  // We have to use `asInstanceOf` because there is no easy way to prove
  // to the compiler that we are returning a T even though we know that we are
  // since T can only be one of `Abs` or `Rel` and we used `refineTypeAbs`
  // to check which one it is
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def sandboxCurrent[A,T](path: Path[A,T,Unsandboxed]): Option[Path[A,T,Sandboxed]] =
    refineTypeAbs(path).fold(
      abs => (abs relativeTo root).map(p => (root </> p).asInstanceOf[Path[A,T,Sandboxed]]),
      rel => (rel relativeTo  cur).map(p => (cur  </> p).asInstanceOf[Path[A,T,Sandboxed]]))

  // TODO[pathy]: Offer clean API in pathy to do this
  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def refineTypeAbs[T,S](path: Path[_,T,S]): Path[Abs,T,S] \/ Path[Rel,T,S] = {
    if (path.isAbsolute) path.asInstanceOf[Path[Abs,T,S]].left
    else path.asInstanceOf[Path[Rel,T,S]].right
  }

  def mkAbsolute[T,S](baseDir: AbsDir[S], path: Path[_,T,S]): Path[Abs,T,S] =
    refineTypeAbs(path).fold(ι, baseDir </> _)

  ////

  private val root = rootDir[Sandboxed]
  private val cur  = currentDir[Sandboxed]
}

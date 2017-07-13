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

package quasar.contrib.pathy

import slamdata.Predef._

import org.scalacheck.{Arbitrary, Gen, Shrink}
import pathy.Path._
import pathy.scalacheck.PathyArbitrary._

trait PathArbitrary {

  implicit val arbitraryAPath: Arbitrary[APath] =
    Arbitrary(Gen.oneOf(Arbitrary.arbitrary[AFile], Arbitrary.arbitrary[ADir]))

  implicit val arbitraryRPath: Arbitrary[RPath] =
    Arbitrary(Gen.oneOf(Arbitrary.arbitrary[RFile], Arbitrary.arbitrary[RDir]))

  implicit val arbitraryFPath: Arbitrary[FPath] =
    Arbitrary(Gen.oneOf(Arbitrary.arbitrary[AFile], Arbitrary.arbitrary[RFile]))

  implicit val arbitraryDPath: Arbitrary[DPath] =
    Arbitrary(Gen.oneOf(Arbitrary.arbitrary[ADir], Arbitrary.arbitrary[RDir]))

  implicit val shrinkAFile: Shrink[AFile] = Shrink { aFile =>
    if (depth(aFile) <= 1) Stream.empty
    else (1 until depth(aFile)).map(removeDirAt(aFile, _)).toStream
  }

  private def removeDirAt(file: AFile, index: Int): AFile = {
    val (dir, rFile) = split(file, index)
    parentDir(dir).getOrElse(dir) </> rFile
  }

  private def split(file: AFile, index: Int): (ADir, RFile) = {
    // NB: `.get` should always succeed because it is impossible not to sandbox
    //     an absolute path to the root directory
    if (index <= 0) (rootDir[Sandboxed], sandbox(rootDir[Sandboxed], file).get)
    else {
      val (dir, filename) = peel(file)
      peel(dir).map { case (parent, dirname) =>
        val (dir, file) = split(parent </> file1(filename), index - 1)
        (dir </> dir1(dirname), file)
      }.getOrElse((rootDir[Sandboxed], sandbox(rootDir[Sandboxed], file).get))
    }
  }

  private def peel(file: AFile): (ADir, FileName) =
    (fileParent(file), fileName(file))

  private def peel(dir: ADir): Option[(ADir, DirName)] =
    parentDir(dir).zip(dirName(dir)).headOption
}

object PathArbitrary extends PathArbitrary

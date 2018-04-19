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

package quasar.fs

import slamdata.Predef._
import quasar.contrib.pathy._

import pathy.Path._
import scalaz._, Scalaz._

/**
  * Use with care. Functions make the assumption that Sandboxed Pathy paths do
  * not contain ParentIn or Current. This can not currently be guaranteed.
  */
object SandboxedPathy {

  implicit val fileNameEqual: Equal[FileName] = Equal.equalA
  implicit val dirNameEqual: Equal[DirName] = Equal.equalA

  def rootSubPath(depth: Int, p: APath): APath = {
    val elems = flatten(none, none, none, DirName(_).left.some, FileName(_).right.some, p).toList.unite
    val dirs = elems.collect { case -\/(e) => e }
    val file = elems.collect { case \/-(e) => e }.headOption

    def dirsPath(dirs: List[DirName]) = dirs.foldLeft(rootDir){ case (a, e) => a </> dir1(e) }

    if (depth > dirs.size) {
      val p = dirsPath(dirs)
      file.cata(p </> file1(_), p)
    }
    else
      dirsPath(dirs.take(depth))
  }

  def largestCommonPathFromRoot(a: APath, b: APath): APath = {
    val i = (0 until (depth(a) max depth(b))).find(i => segAt(i, a) =/= segAt(i, b))

    rootSubPath(i.getOrElse(0), a)
  }

  def segAt[B,T,S](index: Int, path: pathy.Path[B,T,S]): Option[PathSegment] = {
    scala.Predef.require(index >= 0)
    val list =
      pathy.Path.flatten(none, none, none, DirName(_).left.some,FileName(_).right.some, path).toIList.unite
    list.drop(index).headOption
  }

}

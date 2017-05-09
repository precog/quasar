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

package quasar.yggdrasil
package util

import quasar.blueeyes._
import quasar.precog.common._
import quasar.blueeyes.json._

object CPathUtils {
  def cPathToJPaths(cpath: CPath, value: CValue): List[(JPath, CValue)] = (cpath.nodes, value) match {
    case (CPathField(name) :: tail, _) => addComponent(JPathField(name), cPathToJPaths(CPath(tail), value))
    case (CPathIndex(i) :: tail, _)    => addComponent(JPathIndex(i), cPathToJPaths(CPath(tail), value))
    case (CPathArray :: tail, es: CArray[_]) =>
      val CArrayType(elemType) = es.cType
      es.value.toList.zipWithIndex flatMap { case (e, i) => addComponent(JPathIndex(i), cPathToJPaths(CPath(tail), elemType(e))) }
    // case (CPathMeta(_) :: _, _) => Nil
    case (Nil, _)  => List((NoJPath, value))
    case (path, _) => sys.error("Bad news, bob! " + path)
  }

  private def addComponent(c: JPathNode, xs: List[(JPath, CValue)]): List[(JPath, CValue)] = xs map {
    case (path, value) => (JPath(c :: path.nodes), value)
  }

  /**
    * Returns the intersection of `cPath1` and `cPath2`. If there are no
    * `CPathArray` components in the 2 paths, then the intersection is non-empty
    * iff `cPath1 === cPath2`. However, if `cPath1` and/or `cPath2` contain some
    * `CPathArray` components, then they intersect if we can replace some of the
    * `CPathArray`s with `CPathIndex(i)` and have them be equal. This is `CPath`
    * is their intersection.
    *
    * For instance, `intersect(CPath("a.b[*].c[0]"), CPath(CPath("a.b[3].c[*]")) === CPath("a.b[3].c[0]")`.
    */
  def intersect(cPath1: CPath, cPath2: CPath): Option[CPath] = {

    @scala.annotation.tailrec
    def loop(ps1: List[CPathNode], ps2: List[CPathNode], matches: List[CPathNode]): Option[CPath] = (ps1, ps2) match {
      case (Nil, Nil) =>
        Some(CPath(matches.reverse))
      case (p1 :: ps1, p2 :: ps2) if p1 == p2 =>
        loop(ps1, ps2, p1 :: matches)
      case (CPathArray :: ps1, (p2: CPathIndex) :: ps2) =>
        loop(ps1, ps2, p2 :: matches)
      case ((p1: CPathIndex) :: ps1, CPathArray :: ps2) =>
        loop(ps1, ps2, p1 :: matches)
      case _ =>
        None
    }

    loop(cPath1.nodes, cPath2.nodes, Nil)
  }

  // TODO Not really a union.
  def union(cPath1: CPath, cPath2: CPath): Option[CPath] = {
    def loop(ps1: List[CPathNode], ps2: List[CPathNode], acc: List[CPathNode]): Option[CPath] = (ps1, ps2) match {
      case (Nil, Nil) =>
        Some(CPath(acc.reverse))
      case (p1 :: ps1, p2 :: ps2) if p1 == p2 =>
        loop(ps1, ps2, p1 :: acc)
      case (CPathArray :: ps1, (_: CPathIndex) :: ps2) =>
        loop(ps1, ps2, CPathArray :: acc)
      case ((_: CPathIndex) :: ps1, CPathArray :: ps2) =>
        loop(ps1, ps2, CPathArray :: acc)
      case _ =>
        None
    }

    loop(cPath1.nodes, cPath2.nodes, Nil)
  }
}

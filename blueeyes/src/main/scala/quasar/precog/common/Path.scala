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

package quasar.precog.common

import quasar.blueeyes._, json._, serialization._, DefaultSerialization._
import scalaz._, Scalaz._

class Path private (val elements: String*) {
  def components: List[String] = elements.toList
  val path: String = elements.mkString("/", "/", "/").replaceAll("/+", "/")
  val length: Int  = elements.length

  lazy val parent: Option[Path] = elements.size match {
    case 0 => None
    case 1 => Some(Path.Root)
    case _ => Some(new Path(elements.init: _*))
  }

  lazy val ancestors: List[Path] = {
    val parentList = parent.toList

    parentList ++ parentList.flatMap(_.ancestors)
  }

  def /(that: Path)               = new Path(elements ++ that.elements: _*)
  def -(that: Path): Option[Path] = elements.startsWith(that.elements).option(new Path(elements.drop(that.elements.length): _*))

  def isEqualOrParentOf(that: Path) = that.elements.startsWith(this.elements)

  def isChildOf(that: Path) = elements.startsWith(that.elements) && length > that.length

  def rollups(depth: Int): List[Path] = this :: ancestors.take(depth)

  def urlEncode: Path = new Path(elements.map(java.net.URLEncoder.encode(_, "UTF-8")): _*)

  def prefix: Option[Path] = elements.nonEmpty.option(Path(components.init))

  override def equals(that: Any) = that match {
    case Path(`path`) => true
    case _            => false
  }

  override def hashCode = path.hashCode

  override def toString = path
}

object Path {
  implicit val PathDecomposer: Decomposer[Path] = StringDecomposer contramap { (_: Path).toString }
  implicit val PathExtractor: Extractor[Path]   = StringExtractor map { Path(_) }

  val Root = new Path()

  private def cleanPath(string: String): String = string.replaceAll("^/|/$", "").replaceAll("/+", "/")

  def apply(path: String): Path = new Path(cleanPath(path).split("/").filterNot(_.trim.isEmpty): _*)

  def apply(elements: List[String]): Path = apply(elements.mkString("/"))

  def unapply(path: Path): Option[String] = Some(path.path)
}

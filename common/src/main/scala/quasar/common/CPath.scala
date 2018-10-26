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

package quasar.common

import slamdata.Predef._

import scalaz.{Cord, Show}
import scalaz.Ordering._
import scalaz.std.anyVal._
import scalaz.std.list._
import scalaz.std.string._
import scalaz.syntax.show._

@SuppressWarnings(Array("org.wartremover.warts.Overloading"))
sealed trait CPath { self =>
  def nodes: List[CPathNode]

  def \(that: CPath): CPath = CPath(self.nodes ++ that.nodes)
  def \(that: String): CPath = CPath(self.nodes :+ CPathField(that))
  def \(that: Int): CPath = CPath(self.nodes :+ CPathIndex(that))

  def \:(that: CPath): CPath = CPath(that.nodes ++ self.nodes)
  def \:(that: String): CPath = CPath(CPathField(that) +: self.nodes)
  def \:(that: Int): CPath = CPath(CPathIndex(that) +: self.nodes)

  def hasPrefixComponent(p: CPathNode): Boolean = nodes.startsWith(p :: Nil)
  def hasPrefix(p: CPath): Boolean = nodes.startsWith(p.nodes)

  def dropPrefix(p: CPath): Option[CPath] = {
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def remainder(nodes: List[CPathNode], toDrop: List[CPathNode]): Option[CPath] = {
      nodes match {
        case x :: xs =>
          toDrop match {
            case `x` :: ys => remainder(xs, ys)
            case Nil => Some(CPath(nodes))
            case _ => None
          }

        case Nil =>
          if (toDrop.isEmpty) Some(CPath(nodes))
          else None
      }
    }

    remainder(nodes, p.nodes)
  }

  def head: Option[CPathNode] = nodes.headOption
  def tail: CPath = CPath(nodes.drop(1): _*)

  // this is required by a few tests in niflheim
  override def toString = if (nodes.isEmpty) "." else nodes.mkString("")
}

@SuppressWarnings(Array("org.wartremover.warts.Overloading"))
sealed trait CPathNode {
  def \(that: CPath) = CPath(this :: that.nodes)
  def \(that: CPathNode) = CPath(this :: that :: Nil)
}

object CPathNode {
  implicit object CPathNodeOrder extends scalaz.Order[CPathNode] {
    @SuppressWarnings(Array("org.wartremover.warts.Equals"))
    def order(n1: CPathNode, n2: CPathNode): scalaz.Ordering = (n1, n2) match {
      case (CPathField(s1), CPathField(s2)) => scalaz.Ordering.fromInt(s1.compare(s2))
      case (CPathField(_), _) => GT
      case (_, CPathField(_)) => LT

      case (CPathArray, CPathArray) => EQ
      case (CPathArray, _) => GT
      case (_, CPathArray) => LT

      case (CPathIndex(i1), CPathIndex(i2)) => if (i1 < i2) LT else if (i1 == i2) EQ else GT
      case (CPathIndex(_), _) => GT
      case (_, CPathIndex(_)) => LT

      case (CPathMeta(m1), CPathMeta(m2)) => scalaz.Ordering.fromInt(m1.compare(m2))
    }
  }
}

final case class CPathField(name: String) extends CPathNode {
  override def toString = "." + name
}

final case class CPathMeta(name: String) extends CPathNode {
  override def toString = "@" + name
}

final case class CPathIndex(index: Int) extends CPathNode {
  override def toString = "[" + index.toString + "]"
}

final case object CPathArray extends CPathNode {
  override def toString = "[*]"
}

@SuppressWarnings(Array("org.wartremover.warts.Overloading"))
object CPath {

  private[this] final case class CompositeCPath(nodes: List[CPathNode]) extends CPath

  val Identity = apply()

  def apply(n: CPathNode*): CPath = CompositeCPath(n.toList)

  def apply(l: List[CPathNode]): CPath = apply(l: _*)

  def unapplySeq(path: CPath): Option[List[CPathNode]] = Some(path.nodes)

  def unapplySeq(path: String): Option[List[CPathNode]] = Some(parse(path).nodes)

  // TODO parse CPathMeta
  def parse(path: String): CPath = {
    val PathPattern = """\.|(?=\[\d+\])|(?=\[\*\])""".r
    val IndexPattern = """^\[(\d+)\]$""".r

    @SuppressWarnings(Array(
      "org.wartremover.warts.Equals",
      "org.wartremover.warts.Recursion"))
    def parse0(segments: List[String], acc: List[CPathNode]): List[CPathNode] = segments match {
      case Nil => acc

      case head :: tail =>
        if (head.trim.length == 0) {
          parse0(tail, acc)
        } else {
          val next: CPathNode = head match {
            case "[*]" => CPathArray
            case IndexPattern(index) => CPathIndex(index.toInt)
            case name => CPathField(name)
          }
          parse0(tail, next :: acc)
        }
    }

    val properPath: String = if (path.startsWith(".")) path else "." + path

    apply(parse0(PathPattern.split(properPath).toList, Nil).reverse: _*)
  }

  implicit val cpathNodeShow: Show[CPathNode] = Show.show {
    case CPathField(name) => Cord("CPathField(") ++ name.show ++ Cord(")")
    case CPathIndex(idx) => Cord("CPathIndex(") ++ idx.show ++ Cord(")")
    case CPathArray => Cord("CPathArray()")
    case CPathMeta(name) => Cord("CPathMeta(") ++ name.show ++ Cord(")")
  }

  implicit val cPathShow: Show[CPath] = Show.show {
    case CompositeCPath(nodes) => nodes.show
  }

  implicit object CPathOrder extends scalaz.Order[CPath] {
    def order(v1: CPath, v2: CPath): scalaz.Ordering = {
      @SuppressWarnings(Array(
        "org.wartremover.warts.Equals",
        "org.wartremover.warts.Recursion"))
      def compare0(n1: List[CPathNode], n2: List[CPathNode]): scalaz.Ordering = (n1, n2) match {
        case (Nil, Nil) => EQ
        case (Nil, _) => LT
        case (_, Nil) => GT

        case (n1 :: ns1, n2 :: ns2) =>
          val ncomp = scalaz.Order[CPathNode].order(n1, n2)
          if (ncomp != EQ) ncomp else compare0(ns1, ns2)
      }

      compare0(v1.nodes, v2.nodes)
    }
  }

  implicit val CPathOrdering: scala.Ordering[CPath] = CPathOrder.toScalaOrdering
}

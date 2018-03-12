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

package quasar

import slamdata.Predef._
import quasar.fp._
import quasar.fp.ski._

import argonaut._, Argonaut._
import matryoshka._
import scalaz._, Scalaz._

final case class RenderedTree(nodeType: List[String], label: Option[String], children: List[RenderedTree]) {
  def simpleType: Option[String] = nodeType.headOption

  def retype(f: List[String] => List[String]) = this.copy(nodeType = f(nodeType))

  /** A tree that describes differences between two trees:
    * - If the two trees are identical, the result is the same as (either) input.
    * - If the trees differ only in the labels on nodes, then the result has those
    *   nodes decorated with "[Changed] old -> new".
    * - If a single node is unmatched on either side, it is decorated with "[Added]"
    *   or "[Deleted]".
    * As soon as a difference is found and decorated, the subtree(s) beneath the
    * decorated nodes are not inspected.
    *
    * Node types are not compared or necessarily preserved.
    */
  def diff(that: RenderedTree): RenderedTree = {
    def prefixedType(t: RenderedTree, p: String): List[String] = t.nodeType match {
      case first :: rest => (p + " " + first) :: rest
      case Nil           => p :: Nil
    }

    def prefixType(t: RenderedTree, p: String): RenderedTree = t.copy(nodeType = prefixedType(t, p))
    val deleted = ">>>"
    val added = "<<<"

    (this, that) match {
      case (RenderedTree(nodeType1, l1, children1), RenderedTree(nodeType2, l2, children2)) => {
        if (nodeType1 =/= nodeType2 || l1 =/= l2)
          RenderedTree(List("[Root differs]"), None,
            prefixType(this, deleted) ::
            prefixType(that, added) ::
            Nil)
        else {
          @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
          def matchChildren(children1: List[RenderedTree], children2: List[RenderedTree]): List[RenderedTree] = (children1, children2) match {
            case (Nil, Nil)     => Nil
            case (x :: xs, Nil) => prefixType(x, deleted) :: matchChildren(xs, Nil)
            case (Nil, x :: xs) => prefixType(x, added) :: matchChildren(Nil, xs)

            case (a :: as, b :: bs)        if a.typeAndLabel ≟ b.typeAndLabel  => a.diff(b) :: matchChildren(as, bs)
            case (a1 :: a2 :: as, b :: bs) if a2.typeAndLabel ≟ b.typeAndLabel => prefixType(a1, deleted) :: a2.diff(b) :: matchChildren(as, bs)
            case (a :: as, b1 :: b2 :: bs) if a.typeAndLabel ≟ b2.typeAndLabel => prefixType(b1, added) :: a.diff(b2) :: matchChildren(as, bs)

            case (a :: as, b :: bs) => prefixType(a, deleted) :: prefixType(b, added) :: matchChildren(as, bs)
          }
          RenderedTree(nodeType1, l1, matchChildren(children1, children2))
        }
      }
    }
  }

  /**
  A 2D String representation of this Tree, separated into lines. Based on
  scalaz Tree's show, but improved to use a single line per node, use
  unicode box-drawing glyphs, and to handle newlines in the rendered
  nodes.
  */
  def draw: Stream[String] = {
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def drawSubTrees(s: List[RenderedTree]): Stream[String] = s match {
      case Nil      => Stream.Empty
      case t :: Nil => shift("╰─ ", "   ", t.draw)
      case t :: ts  => shift("├─ ", "│  ", t.draw) append drawSubTrees(ts)
    }
    def shift(first: String, other: String, s: Stream[String]): Stream[String] =
      (first #:: Stream.continually(other)).zip(s).map {
        case (a, b) => a + b
      }

    val (prefix, body, suffix) = (simpleType, label) match {
      case (None,             None)        => ("", "", "")
      case (None,             Some(label)) => ("", label, "")
      case (Some(simpleType), None)        => ("", simpleType, "")
      case (Some(simpleType), Some(label)) => (simpleType + "(",  label, ")")
    }
    val indent = " " * (prefix.length-2)
    val lines = body.split("\n")
    lines.zipWithIndex.map { case (a, index) =>
      def first = index == 0
      def last = index == lines.length - 1
      val pre = if (first) prefix else indent
      val suf = if (last) suffix else ""
      pre + a + suf
    } ++: drawSubTrees(children)
  }

  private def typeAndLabel: String = (simpleType, label) match {
    case (None,             None)        => ""
    case (None,             Some(label)) => label
    case (Some(simpleType), None)        => simpleType
    case (Some(simpleType), Some(label)) => simpleType + "(" + label + ")"
  }
}

object RenderedTree {
  implicit val RenderedTreeShow: Show[RenderedTree] = new Show[RenderedTree] {
    override def show(t: RenderedTree) = t.draw.mkString("\n")
  }

  implicit val RenderedTreeEncodeJson: EncodeJson[RenderedTree] = EncodeJson {
    case RenderedTree(nodeType, label, children) =>
      Json.obj((
        (nodeType match {
          case Nil => None
          case _   => Some("type" := nodeType.reverse.mkString("/"))
        }) ::
          Some("label" := label) ::
          {
            if (children.empty) None
            else Some("children" := children.map(RenderedTreeEncodeJson.encode(_)))
          } ::
          Nil).foldMap(_.toList): _*)
  }

  implicit val renderTree: RenderTree[RenderedTree] = RenderTree.make(ι)

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  implicit val equal: Equal[RenderedTree] =
    new Equal[RenderedTree] {
      def equal(x: RenderedTree, y: RenderedTree) =
        (x.nodeType ≟ y.nodeType) && (x.label ≟ y.label) && (x.children ≟ y.children)
    }
}

object Terminal {
  def apply(nodeType: List[String], label: Option[String])
      : RenderedTree =
    RenderedTree(nodeType, label, Nil)

  def opt(nodeType: List[String], label: Option[String])
      : Option[RenderedTree] =
    label.map(l => Terminal(nodeType, Some(l)))
}

object NonTerminal {
  def apply(nodeType: List[String], label: Option[String], children: List[RenderedTree])
      : RenderedTree =
    RenderedTree(nodeType, label, children)
}

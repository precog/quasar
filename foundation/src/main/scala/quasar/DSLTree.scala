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

package quasar

import slamdata.Predef._

import scalaz._

final case class DSLTree(base: String, label: String, children: Option[List[String \/ DSLTree]])

object DSLTree {
  implicit val dslTreeShow: Show[DSLTree] = new Show[DSLTree] {
    def indentString(i: Int): String = java.lang.String.copyValueOf(Array.fill(i)(' '))
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    override def show(f: DSLTree): Cord = {
      def showArg(arg: String \/ DSLTree, indent: Int): Cord = {
        indentString(indent + 1) +: arg.fold(Cord(_), showIndent(_, indent + 1))
      }
      def showArgs(args: List[String \/ DSLTree], indent: Int): Cord = {
        NonEmptyList.lift[String \/ DSLTree, Cord] { nel =>
          val firstArgEnd =
            if (nel.tail.nonEmpty) ",\n" + indentString(indent + 1)
            else ""
          (s"(\n${indentString(indent + 1)}" +: showArg(nel.head, indent) :+ firstArgEnd) ++
            nel.tail.foldLeft(Cord.empty) { (b, a) =>
              (if (b.nonEmpty) b :+ firstArgEnd else b) ++ showArg(a, indent)
            } :- ')'
        }(IList.fromList(args)).getOrElse(Cord("()"))
      }
      def showIndent(f: DSLTree, indent: Int) =
        f match {
          case DSLTree(base, label, children) =>
            val args = children.fold(Cord.empty)(showArgs(_, indent))
            if (base.isEmpty && label.isEmpty) args
            else if (base.isEmpty) Cord(label) ++ args
            else Cord(base + "." + label) ++ args
        }
      showIndent(f, 0)
    }
  }
}

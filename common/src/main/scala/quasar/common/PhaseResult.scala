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

package quasar.common

import slamdata.Predef._
import quasar.{DSLTree, NonTerminal, RenderedTree, RenderDSL, RenderTree, Terminal}
import RenderTree.ops._, RenderDSL.ops._

import argonaut._, Argonaut._
import scalaz.syntax.show._

sealed abstract class PhaseResult {
  import PhaseResult._
  def name: String
  def showTree: String = this match {
    case Tree(name, value, _) => name + ":\n" + value.shows
    case Detail(name, value) => name + ":\n" + value
  }
  def showCode: String = this match {
    case Tree(name, _, Some(code)) => name + ":\n" + code.shows
    case Tree(name, value, None) => name + ":\n" + value.shows
    case Detail(name, value) => name + ":\n" + value
  }
}

object PhaseResult {
  final case class Tree(name: String, value: RenderedTree, code: Option[DSLTree]) extends PhaseResult
  final case class Detail(name: String, value: String)     extends PhaseResult

  def tree[A: RenderTree](name: String, value: A): PhaseResult =
    Tree(name, value.render, None)
  def treeAndCode[A: RenderTree: RenderDSL](name: String, value: A): PhaseResult =
    Tree(name, value.render, Some(value.toDsl))
  def detail(name: String, value: String): PhaseResult = Detail(name, value)

  implicit def renderTree: RenderTree[PhaseResult] = new RenderTree[PhaseResult] {
    def render(v: PhaseResult) = v match {
      case Tree(name, value, _) => NonTerminal(List("PhaseResult"), Some(name), List(value))
      case Detail(name, value)  => NonTerminal(List("PhaseResult"), Some(name), List(Terminal(List("Detail"), Some(value))))
    }
  }

  implicit def phaseResultEncodeJson: EncodeJson[PhaseResult] = EncodeJson {
    case Tree(name, value, _) => Json.obj("name" := name, "tree" := value)
    case Detail(name, value)  => Json.obj("name" := name, "detail" := value)
  }
}

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

import quasar.Predef._
import quasar.{NonTerminal, RenderedTree, RenderTree, Terminal}, RenderTree.ops._

import argonaut._, Argonaut._
import scalaz.Show
import scalaz.syntax.show._

sealed trait PhaseResult {
  def name: String
  def time: Long
}

object PhaseResult {
  private final case class Tree(name: String, time: Long, value: RenderedTree) extends PhaseResult
  private final case class Detail(name: String, time: Long, value: String)     extends PhaseResult

  def tree[A: RenderTree](name: String, value: A): PhaseResult =
    Tree(name, java.lang.System.nanoTime, value.render)
  def detail(name: String, value: String): PhaseResult =
    Detail(name, java.lang.System.nanoTime, value)

  def asJson(pr: PhaseResult): Json = pr match {
    case Tree(_, _, value)   => value.asJson
    case Detail(_, _, value) => value.asJson
  }

  implicit def show: Show[PhaseResult] = Show.shows {
    case Tree(name, _, value)   => name + ":\n" + value.shows
    case Detail(name, _, value) => name + ":\n" + value
  }

  implicit def renderTree: RenderTree[PhaseResult] = new RenderTree[PhaseResult] {
    def render(v: PhaseResult) = v match {
      case Tree(name, _, value)   => NonTerminal(List("PhaseResult"), Some(name), List(value))
      case Detail(name, _, value) => NonTerminal(List("PhaseResult"), Some(name), List(Terminal(List("Detail"), Some(value))))
    }
  }

  implicit def phaseResultEncodeJson: EncodeJson[PhaseResult] = EncodeJson {
    case Tree(name, _, value)   => Json.obj("name" := name, "tree" := value)
    case Detail(name, _, value) => Json.obj("name" := name, "detail" := value)
  }
}

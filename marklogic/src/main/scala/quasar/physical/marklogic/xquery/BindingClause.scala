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

package quasar.physical.marklogic.xquery

import slamdata.Predef._

import monocle.Prism
import scalaz.NonEmptyList
import scalaz.std.string._
import scalaz.syntax.foldable._

/** https://www.w3.org/TR/xquery/#id-for-let */
sealed abstract class BindingClause {
  import BindingClause._

  def render: String = this match {
    case ForClause(bs) => "for " + bs.map(_.render("in")).intercalate(", ")
    case LetClause(bs) => "let " + bs.map(_.render(":=")).intercalate(", ")
  }
}

object BindingClause {
  final case class ForClause(bindings: NonEmptyList[PositionalBinding]) extends BindingClause
  final case class LetClause(bindings: NonEmptyList[Binding]) extends BindingClause

  val forClause = Prism.partial[BindingClause, NonEmptyList[PositionalBinding]] {
    case ForClause(bindings) => bindings
  } (ForClause)

  val letClause = Prism.partial[BindingClause, NonEmptyList[Binding]] {
    case LetClause(bindings) => bindings
  } (LetClause)

  def for_(b: PositionalBinding, bs: PositionalBinding*): BindingClause =
    forClause(NonEmptyList(b, bs: _*))

  def let_(b: Binding, bs: Binding*): BindingClause =
    letClause(NonEmptyList(b, bs: _*))
}

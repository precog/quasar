/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.qsu
package minimizers

import slamdata.Predef._

import quasar.{NonTerminal, RenderTree, RenderTreeT, Terminal}
import quasar.RenderTree.ops._
import quasar.contrib.scalaz.nel._

import matryoshka._

import scalaz.{Equal, Foldable, NonEmptyList, Show}
import scalaz.std.option._
import scalaz.syntax.foldable._

private[minimizers] sealed trait Cartouche[T[_[_]], +P, +S, +O] extends Product with Serializable {
  def dropHead: Cartouche[T, P, S, O]
  def isEmpty: Boolean
  def length: Int
  def ::[PP >: P, SS >: S, OO >: O](stage: CStage[T, PP, SS, OO]): Cartouche[T, PP, SS, OO]
}

private[minimizers] object Cartouche {

  private[minimizers] final case class Stages[T[_[_]], P, S, O](
      stages: NonEmptyList[CStage[T, P, S, O]])
      extends Cartouche[T, P, S, O] {

    def dropHead: Cartouche[T, P, S, O] =
      Cartouche.fromFoldable(stages.tail)

    val isEmpty: Boolean = false

    def length: Int = stages.length

    def ::[PP >: P, SS >: S, OO >: O](stage: CStage[T, PP, SS, OO]): Cartouche[T, PP, SS, OO] =
      Cartouche.stages(stage <:: stages.widen[CStage[T, PP, SS, OO]])
  }

  private[minimizers] final case class Source[T[_[_]], P, S, O]()
      extends Cartouche[T, P, S, O] {
    val dropHead: Cartouche[T, P, S, O] = this
    val isEmpty: Boolean = true
    val length: Int = 0
    def ::[PP >: P, SS >: S, OO >: O](stage: CStage[T, PP, SS, OO]): Cartouche[T, PP, SS, OO] =
      Cartouche.stages(NonEmptyList(stage))
  }

  def source[T[_[_]], P, S, O]: Cartouche[T, P, S, O] =
    Source[T, P, S, O]()

  def stages[T[_[_]], P, S, O](ss: NonEmptyList[CStage[T, P, S, O]]): Cartouche[T, P, S, O] =
    Stages(ss)

  def fromFoldable[F[_]: Foldable, T[_[_]], P, S, O](fa: F[CStage[T, P, S, O]])
      : Cartouche[T, P, S, O] =
    fa.foldMapRight1Opt(NonEmptyList(_))((s, ss) => s <:: ss)
      .fold(source[T, P, S, O])(stages(_))

  implicit def renderTree[T[_[_]]: RenderTreeT: ShowT, P: Show, S: RenderTree, O: Show]
      : RenderTree[Cartouche[T, P, S, O]] =
    RenderTree make {
      case Source() =>
        Terminal(List("Source"), None)

      case Stages(ss) =>
        NonTerminal(List("Cartouche"), None, ss.widen[CStage[T, P, S, O]].toList.map(_.render))
    }

  implicit def equal[T[_[_]]: BirecursiveT: EqualT, P: Equal, S: Equal, O: Equal]
      : Equal[Cartouche[T, P, S, O]] =
    Equal.equalBy[Cartouche[T, P, S, O], Option[NonEmptyList[CStage[T, P, S, O]]]] {
      case Source() => None
      case Stages(ss) => Some(ss.widen[CStage[T, P, S, O]])
    }
}

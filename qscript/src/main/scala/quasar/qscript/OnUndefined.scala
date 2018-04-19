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

package quasar.qscript

import slamdata.Predef.{Boolean, Option, Some}
import quasar.RenderTree

import scalaz.{Enum, Show}
import scalaz.std.anyVal._
import scalaz.syntax.order._

sealed abstract class OnUndefined {
  def fold[A](emit: => A, omit: => A): A =
    this match {
      case OnUndefined.Emit => emit
      case OnUndefined.Omit => omit
    }
}

object OnUndefined {
  case object Emit extends OnUndefined
  case object Omit extends OnUndefined

  val emit: OnUndefined = Emit
  val omit: OnUndefined = Omit

  implicit val enum: Enum[OnUndefined] =
    new Enum[OnUndefined] {
      def order(x: OnUndefined, y: OnUndefined) =
        asBool(x) ?|? asBool(y)

      def pred(x: OnUndefined): OnUndefined =
        x.fold(omit, emit)

      def succ(x: OnUndefined): OnUndefined =
        x.fold(omit, emit)

      override val min: Option[OnUndefined] =
        Some(Emit)

      override val max: Option[OnUndefined] =
        Some(Omit)

      private val asBool: OnUndefined => Boolean =
        _.fold(false, true)
    }

  implicit def renderTree: RenderTree[OnUndefined] =
    RenderTree.fromShow("OnUndefined")

  implicit def show: Show[OnUndefined] =
    Show.showFromToString
}

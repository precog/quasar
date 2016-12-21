/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.physical.mongodb.planner

import quasar.Predef._
import quasar.frontend.logicalplan.LogicalPlan

import matryoshka.data._
import matryoshka.implicits._
import scalaz.Cofree

sealed abstract class InputFinder {
  def apply[A](t: Cofree[LogicalPlan, A]): A
}

case object Here extends InputFinder {
  def apply[A](a: Cofree[LogicalPlan, A]): A =
    a.head
}

final case class There(index: Int, next: InputFinder) extends InputFinder {
  def apply[A](a: Cofree[LogicalPlan, A]): A =
    next(a.children.apply(index))
}

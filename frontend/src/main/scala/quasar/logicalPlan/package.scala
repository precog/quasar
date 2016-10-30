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

package quasar

import quasar.Predef._
import quasar.contrib.pathy.FPath
import quasar.namegen.NameGen

import scala.Symbol

import monocle.Prism
import shapeless.Nat
import scalaz._

package object logicalPlan {
  def read[A] =
    Prism.partial[LogicalPlan[A], FPath] { case Read(p) => p } (Read(_))

  def constant[A] =
    Prism.partial[LogicalPlan[A], Data] { case Constant(p) => p } (Constant(_))

  def invoke[A] =
    Prism.partial[LogicalPlan[A], (GenericFunc[Nat], Func.Input[A, Nat])] {
      case Invoke(f, as) => (f, as)
    } ((Invoke[A, Nat](_, _)).tupled)

  def free[A] =
    Prism.partial[LogicalPlan[A], Symbol] { case Free(n) => n } (Free(_))

  def let[A] =
    Prism.partial[LogicalPlan[A], (Symbol, A, A)] {
      case Let(v, f, b) => (v, f, b)
    } ((Let[A](_, _, _)).tupled)

  def typecheck[A] =
    Prism.partial[LogicalPlan[A], (A, Type, A, A)] {
      case Typecheck(e, t, c, f) => (e, t, c, f)
    } ((Typecheck[A](_, _, _, _)).tupled)

  def freshName(prefix: String): State[NameGen, Symbol] =
    quasar.namegen.freshName(prefix).map(Symbol(_))
}

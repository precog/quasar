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

import matryoshka._
import scalaz._
import shapeless._

sealed trait DimensionalEffect
/** Describes a function that reduces a set of values to a single value. */
final case object Reduction extends DimensionalEffect
/** Describes a function that expands a compound value into a set of values for
  * an operation.
  */
final case object Expansion extends DimensionalEffect
/** Describes a function that each individual value. */
final case object Mapping extends DimensionalEffect
/** Describes a function that compresses the identity information. */
final case object Squashing extends DimensionalEffect
/** Describes a function that operates on the set containing values, not
  * modifying individual values. (EG, filter, sort, take)
  */
final case object Sifting extends DimensionalEffect
/** Describes a function that operates on the set containing values, potentially
  * modifying individual values. (EG, joins).
  */
final case object Transformation extends DimensionalEffect

object DimensionalEffect {
  implicit val equal: Equal[DimensionalEffect] = Equal.equalA[DimensionalEffect]
}

final case class UnaryFunc(
    val effect: DimensionalEffect,
    val name: String,
    val help: String,
    val codomain: Func.Codomain,
    val domain: Func.Domain[nat._1],
    val simplify: Func.Simplifier,
    val typer0: Func.Typer[nat._1],
    val untyper0: Func.Untyper[nat._1]) extends GenericFunc[nat._1] {

  def apply[A](a1: A): LogicalPlan[A] =
    applyGeneric(Func.Input1[A](a1))
}

final case class BinaryFunc(
    val effect: DimensionalEffect,
    val name: String,
    val help: String,
    val codomain: Func.Codomain,
    val domain: Func.Domain[nat._2],
    val simplify: Func.Simplifier,
    val typer0: Func.Typer[nat._2],
    val untyper0: Func.Untyper[nat._2]) extends GenericFunc[nat._2] {

  def apply[A](a1: A, a2: A): LogicalPlan[A] =
    applyGeneric(Func.Input2[A](a1, a2))
}

final case class TernaryFunc(
    val effect: DimensionalEffect,
    val name: String,
    val help: String,
    val codomain: Func.Codomain,
    val domain: Func.Domain[nat._3],
    val simplify: Func.Simplifier,
    val typer0: Func.Typer[nat._3],
    val untyper0: Func.Untyper[nat._3]) extends GenericFunc[nat._3] {

  def apply[A](a1: A, a2: A, a3: A): LogicalPlan[A] =
    applyGeneric(Func.Input3[A](a1, a2, a3))
}

abstract class GenericFunc[N <: Nat] {
  def effect: DimensionalEffect
  def name: String
  def help: String
  def codomain: Func.Codomain
  def domain: Func.Domain[N]
  def simplify: Func.Simplifier
  def typer0: Func.Typer[N]
  def untyper0: Func.Untyper[N]

  def applyGeneric[A](args: Func.Input[A, N]): LogicalPlan[A] =
    LogicalPlan.InvokeF[A, N](this, args)

  final def untpe(tpe: Func.Codomain): Func.VDomain[N] =
    untyper0((domain, codomain), tpe)

  final def tpe(args: Func.Domain[N]): Func.VCodomain =
    typer0(args)

  final def arity: Int = domain.length

  override def toString: String = name
}

trait FuncInstances {
  implicit val FuncRenderTree: RenderTree[GenericFunc[_]] = new RenderTree[GenericFunc[_]] {
    def render(func: GenericFunc[_]) = Terminal("Func" :: Nil, Some(func.name))
  }
}

object Func extends FuncInstances {
  /** This handles rewrites that constant-folding (handled by the typers) can’t.
    * I.e., any rewrite where either the result or one of the relevant arguments
    * is a non-Constant expression. It _could_ cover all the rewrites, but
    * there’s no need to duplicate the cases that must also be handled by the
    * typer.
    */
  trait Simplifier {
    def apply[T[_[_]]: Recursive: Corecursive](orig: LogicalPlan[T[LogicalPlan]]):
        Option[LogicalPlan[T[LogicalPlan]]]
  }

  type Input[A, N <: Nat] = Sized[List[A], N]

  type Domain[N <: Nat] = Input[Type, N]
  type Codomain = Type

  type VDomain[N <: Nat] = ValidationNel[SemanticError, Domain[N]]
  type VCodomain = ValidationNel[SemanticError, Codomain]

  type Typer[N <: Nat] = Domain[N] => VCodomain
  type Untyper[N <: Nat] = ((Domain[N], Codomain), Codomain) => VDomain[N]

  def Input1[A](a1: A): Input[A, nat._1] = Sized[List](a1)
  def Input2[A](a1: A, a2: A): Input[A, nat._2] = Sized[List](a1, a2)
  def Input3[A](a1: A, a2: A, a3: A): Input[A, nat._3] = Sized[List](a1, a2, a3)
}

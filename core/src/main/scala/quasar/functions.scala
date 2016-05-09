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

sealed trait DimensionalEffect
/** Describes a function that reduces a set of values to a single value. */
final case object Reduction extends DimensionalEffect
/** Describes a function that expands a compound value into a set of values for
  * an operation.
  */
final case object Expansion extends DimensionalEffect
/** Describes a function that expands a compound value into a set of values. */
final case object ExpansionFlat extends DimensionalEffect
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

final case class Func(
  effect: DimensionalEffect,
  name: String,
  help: String,
  codomain: Func.Codomain,
  domain: Func.Domain,
  simplify: Func.Simplifier,
  typer0: Func.Typer,
  untyper0: Func.Untyper) {

  def apply[A](args: A*): LogicalPlan[A] =
    LogicalPlan.InvokeF(this, args.toList)

  final def untpe(tpe: Func.Codomain): Func.VDomain =
    untyper0(this, tpe)

  final def tpe(args: Func.Domain): Func.VCodomain =
    typer0(args.toList)

  final def arity: Int = domain.length

  override def toString: String = name
}

trait FuncInstances {
  implicit val FuncRenderTree: RenderTree[Func] = new RenderTree[Func] {
    def render(v: Func) = Terminal("Func" :: Nil, Some(v.name))
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

  type Domain = List[Type]
  type Codomain = Type

  type VDomain = ValidationNel[SemanticError, Domain]
  type VCodomain = ValidationNel[SemanticError, Codomain]

  type Typer = Domain => VCodomain
  type Untyper = (Func, Codomain) => VDomain
}

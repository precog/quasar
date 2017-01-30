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
import quasar.frontend.logicalplan.{LogicalPlan => LP, _}

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

final case class NullaryFunc
  (val effect: DimensionalEffect,
    val help: String,
    val codomain: Func.Codomain,
    val simplify: Func.Simplifier)
    extends GenericFunc[nat._0] {
  val domain = Sized[List]()
  val typer0: Func.Typer[nat._0] = _ => Validation.success(codomain)
  val untyper0: Func.Untyper[nat._0] = {
    case ((funcDomain, _), _) => Validation.success(funcDomain)
  }

  def apply[A](): LP[A] =
    applyGeneric(Sized[List]())
}

final case class UnaryFunc(
    val effect: DimensionalEffect,
    val help: String,
    val codomain: Func.Codomain,
    val domain: Func.Domain[nat._1],
    val simplify: Func.Simplifier,
    val typer0: Func.Typer[nat._1],
    val untyper0: Func.Untyper[nat._1]) extends GenericFunc[nat._1] {

  def apply[A](a1: A): LP[A] =
    applyGeneric(Func.Input1[A](a1))
}

final case class BinaryFunc(
    val effect: DimensionalEffect,
    val help: String,
    val codomain: Func.Codomain,
    val domain: Func.Domain[nat._2],
    val simplify: Func.Simplifier,
    val typer0: Func.Typer[nat._2],
    val untyper0: Func.Untyper[nat._2]) extends GenericFunc[nat._2] {

  def apply[A](a1: A, a2: A): LP[A] =
    applyGeneric(Func.Input2[A](a1, a2))
}

final case class TernaryFunc(
    val effect: DimensionalEffect,
    val help: String,
    val codomain: Func.Codomain,
    val domain: Func.Domain[nat._3],
    val simplify: Func.Simplifier,
    val typer0: Func.Typer[nat._3],
    val untyper0: Func.Untyper[nat._3]) extends GenericFunc[nat._3] {

  def apply[A](a1: A, a2: A, a3: A): LP[A] =
    applyGeneric(Func.Input3[A](a1, a2, a3))
}

sealed abstract class GenericFunc[N <: Nat] {
  def effect: DimensionalEffect
  def help: String
  def codomain: Func.Codomain
  def domain: Func.Domain[N]
  def simplify: Func.Simplifier
  def typer0: Func.Typer[N]
  def untyper0: Func.Untyper[N]

  def applyGeneric[A](args: Func.Input[A, N]): LP[A] =
    Invoke[N, A](this, args)

  final def untpe(tpe: Func.Codomain): Func.VDomain[N] =
    untyper0((domain, codomain), tpe)

  final def tpe(args: Func.Domain[N]): Func.VCodomain =
    typer0(args)

  final def arity: Int = domain.length
}

trait GenericFuncInstances {
  implicit def show[N <: Nat]: Show[GenericFunc[N]] = {
    import std.StdLib._

    Show.shows {
      case agg.Count                      => "Count"
      case agg.Sum                        => "Sum"
      case agg.Min                        => "Min"
      case agg.Max                        => "Max"
      case agg.Avg                        => "Avg"
      case agg.Arbitrary                  => "Arbitrary"
      case array.ArrayLength              => "ArrayLength"
      case date.ExtractCentury            => "ExtractCentury"
      case date.ExtractDayOfMonth         => "ExtractDayOfMonth"
      case date.ExtractDecade             => "ExtractDecade"
      case date.ExtractDayOfWeek          => "ExtractDayOfWeek"
      case date.ExtractDayOfYear          => "ExtractDayOfYear"
      case date.ExtractEpoch              => "ExtractEpoch"
      case date.ExtractHour               => "ExtractHour"
      case date.ExtractIsoDayOfWeek       => "ExtractIsoDayOfWeek"
      case date.ExtractIsoYear            => "ExtractIsoYear"
      case date.ExtractMicroseconds       => "ExtractMicroseconds"
      case date.ExtractMillennium         => "ExtractMillennium"
      case date.ExtractMilliseconds       => "ExtractMilliseconds"
      case date.ExtractMinute             => "ExtractMinute"
      case date.ExtractMonth              => "ExtractMonth"
      case date.ExtractQuarter            => "ExtractQuarter"
      case date.ExtractSecond             => "ExtractSecond"
      case date.ExtractTimezone           => "ExtractTimezone"
      case date.ExtractTimezoneHour       => "ExtractTimezoneHour"
      case date.ExtractTimezoneMinute     => "ExtractTimezoneMinute"
      case date.ExtractWeek               => "ExtractWeek"
      case date.ExtractYear               => "ExtractYear"
      case date.Date                      => "Date"
      case date.Now                       => "Now"
      case date.Time                      => "Time"
      case date.Timestamp                 => "Timestamp"
      case date.Interval                  => "Interval"
      case date.StartOfDay                => "StartOfDay"
      case date.TimeOfDay                 => "TimeOfDay"
      case date.ToTimestamp               => "ToTimestamp"
      case identity.Squash                => "Squash"
      case identity.ToId                  => "ToId"
      case math.Add                       => "Add"
      case math.Multiply                  => "Multiply"
      case math.Power                     => "Power"
      case math.Subtract                  => "Subtract"
      case math.Divide                    => "Divide"
      case math.Negate                    => "Negate"
      case math.Modulo                    => "Modulo"
      case relations.Eq                   => "Eq"
      case relations.Neq                  => "Neq"
      case relations.Lt                   => "Lt"
      case relations.Lte                  => "Lte"
      case relations.Gt                   => "Gt"
      case relations.Gte                  => "Gte"
      case relations.Between              => "Between"
      case relations.IfUndefined          => "IfUndefined"
      case relations.And                  => "And"
      case relations.Or                   => "Or"
      case relations.Not                  => "Not"
      case relations.Cond                 => "Cond"
      case set.Sample                     => "Sample"
      case set.Take                       => "Take"
      case set.Drop                       => "Drop"
      case set.Range                      => "Range"
      case set.Filter                     => "Filter"
      case set.InnerJoin                  => "InnerJoin"
      case set.LeftOuterJoin              => "LeftOuterJoin"
      case set.RightOuterJoin             => "RightOuterJoin"
      case set.FullOuterJoin              => "FullOuterJoin"
      case set.GroupBy                    => "GroupBy"
      case set.Distinct                   => "Distinct"
      case set.DistinctBy                 => "DistinctBy"
      case set.Union                      => "Union"
      case set.Intersect                  => "Intersect"
      case set.Except                     => "Except"
      case set.In                         => "In"
      case set.Within                     => "Within"
      case set.Constantly                 => "Constantly"
      case string.Concat                  => "Concat"
      case string.Like                    => "Like"
      case string.Search                  => "Search"
      case string.Length                  => "Length"
      case string.Lower                   => "Lower"
      case string.Upper                   => "Upper"
      case string.Substring               => "Substring"
      case string.Boolean                 => "Boolean"
      case string.Integer                 => "Integer"
      case string.Decimal                 => "Decimal"
      case string.Null                    => "Null"
      case string.ToString                => "ToString"
      case structural.MakeObject          => "MakeObject"
      case structural.MakeArray           => "MakeArray"
      case structural.ObjectConcat        => "ObjectConcat"
      case structural.ArrayConcat         => "ArrayConcat"
      case structural.ConcatOp            => "ConcatOp"
      case structural.ObjectProject       => "ObjectProject"
      case structural.ArrayProject        => "ArrayProject"
      case structural.DeleteField         => "DeleteField"
      case structural.FlattenMap          => "FlattenMap"
      case structural.FlattenArray        => "FlattenArray"
      case structural.FlattenMapKeys      => "FlattenMapKeys"
      case structural.FlattenArrayIndices => "FlattenArrayIndices"
      case structural.ShiftMap            => "ShiftMap"
      case structural.ShiftArray          => "ShiftArray"
      case structural.ShiftMapKeys        => "ShiftMapKeys"
      case structural.ShiftArrayIndices   => "ShiftArrayIndices"
      case structural.UnshiftMap          => "UnshiftMap"
      case structural.UnshiftArray        => "UnshiftArray"
      case f                              => "unknown function: " + f.help
    }
  }

  implicit def renderTree[N <: Nat]: RenderTree[GenericFunc[N]] =
    RenderTree.fromShow("Func")
}

object GenericFunc extends GenericFuncInstances

object Func {
  /** This handles rewrites that constant-folding (handled by the typers) can’t.
    * I.e., any rewrite where either the result or one of the relevant arguments
    * is a non-Constant expression. It _could_ cover all the rewrites, but
    * there’s no need to duplicate the cases that must also be handled by the
    * typer.
    */
  trait Simplifier {
    def apply[T]
      (orig: LP[T])
      (implicit TR: Recursive.Aux[T, LP], TC: Corecursive.Aux[T, LP])
        : Option[LP[T]]
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

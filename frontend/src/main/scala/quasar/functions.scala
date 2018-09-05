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

package quasar

import slamdata.Predef._
import quasar.frontend.logicalplan.{LogicalPlan => LP, _}

// Needed for shapeless
import scala.Predef._

import matryoshka._
import scalaz._
import shapeless._
import shapeless.syntax.sized._
import shapeless.ops.nat.ToInt

sealed abstract class DimensionalEffect
/** Describes a function that reduces a set of values to a single value. */
final case object Reduction extends DimensionalEffect
/** Describes a function that expands a compound value into a set of values for
  * an operation.
  */
final case object Expansion extends DimensionalEffect
/** Describes a function that maps each individual value. */
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

final case class NullaryFunc(
    val effect: DimensionalEffect,
    val help: String,
    val simplify: Func.Simplifier) extends GenericFunc[nat._0] {

  val arity: Int = 0

  def apply[A](): LP[A] =
    applyGeneric(Sized[List]())
}

final case class UnaryFunc(
    val effect: DimensionalEffect,
    val help: String,
    val simplify: Func.Simplifier) extends GenericFunc[nat._1] {

  val arity: Int = 1

  def apply[A](a1: A): LP[A] =
    applyGeneric(Func.Input1[A](a1))
}

final case class BinaryFunc(
    val effect: DimensionalEffect,
    val help: String,
    val simplify: Func.Simplifier) extends GenericFunc[nat._2] {

  val arity: Int = 2

  def apply[A](a1: A, a2: A): LP[A] =
    applyGeneric(Func.Input2[A](a1, a2))
}

final case class TernaryFunc(
    val effect: DimensionalEffect,
    val help: String,
    val simplify: Func.Simplifier) extends GenericFunc[nat._3] {

  val arity: Int = 3

  def apply[A](a1: A, a2: A, a3: A): LP[A] =
    applyGeneric(Func.Input3[A](a1, a2, a3))
}

sealed abstract class GenericFunc[N <: Nat](implicit toInt: ToInt[N]) { self =>
  def effect: DimensionalEffect
  def help: String
  def simplify: Func.Simplifier
  def arity: Int

  def applyGeneric[A](args: Func.Input[A, N]): LP[A] =
    Invoke[N, A](this, args)

  def applyUnsized[A](args: List[A]): Option[LP[A]] =
    args.sized[N].map(applyGeneric)

  def toFunction[A]: HomomorphicFunction[A, LP[A]] = new HomomorphicFunction[A, LP[A]] {
    def arity = self.arity
    def apply(args: List[A]) = self.applyUnsized(args)
  }
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
      case agg.First                      => "First"
      case agg.Last                       => "Last"
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
      case date.ExtractMicrosecond       => "ExtractMicrosecond"
      case date.ExtractMillennium         => "ExtractMillennium"
      case date.ExtractMillisecond       => "ExtractMillisecond"
      case date.ExtractMinute             => "ExtractMinute"
      case date.ExtractMonth              => "ExtractMonth"
      case date.ExtractQuarter            => "ExtractQuarter"
      case date.ExtractSecond             => "ExtractSecond"
      case date.ExtractTimeZone           => "ExtractTimeZone"
      case date.ExtractTimeZoneHour       => "ExtractTimeZoneHour"
      case date.ExtractTimeZoneMinute     => "ExtractTimeZoneMinute"
      case date.ExtractWeek               => "ExtractWeek"
      case date.ExtractYear               => "ExtractYear"
      case date.Now                       => "Now"
      case date.OffsetDateTime            => "OffsetDateTime"
      case date.OffsetTime                => "OffsetTime"
      case date.OffsetDate                => "OffsetDate"
      case date.LocalDateTime             => "LocalDateTime"
      case date.LocalTime                 => "LocalTime"
      case date.LocalDate                 => "LocalDate"
      case date.Interval                  => "Interval"
      case date.SetTimeZone               => "SetTimeZone"
      case date.SetTimeZoneHour           => "SetTimeZoneHour"
      case date.SetTimeZoneMinute         => "SetTimeZoneMinute"
      case date.StartOfDay                => "StartOfDay"
      case date.TimeOfDay                 => "TimeOfDay"
      case date.ToTimestamp               => "ToTimestamp"
      case identity.Squash                => "Squash"
      case math.Add                       => "Add"
      case math.Abs                       => "Abs"
      case math.Multiply                  => "Multiply"
      case math.Power                     => "Power"
      case math.Subtract                  => "Subtract"
      case math.Divide                    => "Divide"
      case math.Negate                    => "Negate"
      case math.Modulo                    => "Modulo"
      case math.Ceil                      => "Ceil"
      case math.Floor                     => "Floor"
      case math.Trunc                     => "Trunc"
      case math.FloorScale                => "FloorScale"
      case math.CeilScale                 => "CeilScale"
      case math.RoundScale                => "RoundScale"
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
      case set.GroupBy                    => "GroupBy"
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
      case string.Split                   => "Split"
      case string.Boolean                 => "Boolean"
      case string.Integer                 => "Integer"
      case string.Decimal                 => "Decimal"
      case string.Number                  => "Number"
      case string.Null                    => "Null"
      case string.ToString                => "ToString"
      case structural.MakeMap             => "MakeMap"
      case structural.MakeArray           => "MakeArray"
      case structural.Meta                => "Meta"
      case structural.MapConcat           => "MapConcat"
      case structural.ArrayConcat         => "ArrayConcat"
      case structural.ConcatOp            => "ConcatOp"
      case structural.MapProject          => "MapProject"
      case structural.ArrayProject        => "ArrayProject"
      case structural.DeleteKey           => "DeleteKey"
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

  def Input1[A](a1: A): Input[A, nat._1] = Sized[List](a1)
  def Input2[A](a1: A, a2: A): Input[A, nat._2] = Sized[List](a1, a2)
  def Input3[A](a1: A, a2: A, a3: A): Input[A, nat._3] = Sized[List](a1, a2, a3)
}

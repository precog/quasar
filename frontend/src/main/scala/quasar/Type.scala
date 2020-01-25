/*
 * Copyright 2014–2019 SlamData Inc.
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
import quasar.common.data.Data
import quasar.fp._

import scala.Any

import scalaz._, Scalaz._

sealed abstract class Type extends Product with Serializable { self =>
  import Type._

  // FIXME: Using `≟` here causes runtime errors.
  @SuppressWarnings(Array("org.wartremover.warts.Equals"))
  final def ⨿ (that: Type): Type =
    if (this == that) this else Coproduct(this, that)
}

trait TypeInstances {
  import Type._

  @SuppressWarnings(Array("org.wartremover.warts.Equals", "org.wartremover.warts.Recursion"))
  implicit val equal: Equal[Type] = Equal.equal((a, b) => (a, b) match {
    case (Top, Top)
       | (Bottom, Bottom)
       | (Null, Null)
       | (Str, Str)
       | (Int, Int)
       | (Dec, Dec)
       | (Bool, Bool)
       | (OffsetDateTime, OffsetDateTime)
       | (OffsetTime, OffsetTime)
       | (OffsetDate, OffsetDate)
       | (LocalDateTime, LocalDateTime)
       | (LocalTime, LocalTime)
       | (LocalDate, LocalDate)
       | (Interval, Interval) =>
      true
    case (Const(a), Const(b)) => a ≟ b
    case (Arr(as), Arr(bs)) => as ≟ bs
    case (FlexArr(min1, max1, t1), FlexArr(min2, max2, t2)) =>
      min1 ≟ min2 && max1 ≟ max2 && t1 ≟ t2
    case (Obj(v1, u1), Obj(v2, u2)) => v1 ≟ v2 && u1 ≟ u2
    case (a @ Coproduct(_, _), b @ Coproduct(_, _)) => a.equals(b)
    case (_, _) => false
  })

  @SuppressWarnings(Array("org.wartremover.warts.ToString"))
  implicit val show: Show[Type] = Show.show {
    case Const(d) => s"constant value ${d.shows}"
    case Arr(types) => "Arr(" + types.shows + ")"
    case FlexArr(min, max, mbrs) =>
      "FlexArr(" + min.shows + ", " + max.shows + ", "  + mbrs.shows + ")"
    case Obj(assocs, unkns) =>
      "Obj(" + assocs.shows + ", " + unkns.shows + ")"
    case cp @ Coproduct(_, _) =>
      val cos = cp.flatten.map(_.shows)
      cos.init.toList.mkString(", ") + ", or " + cos.last
    case x => x.toString
  }

  implicit val TypeRenderTree: RenderTree[Type] =
    RenderTree.fromShow[Type]("Type")
}

object Type extends TypeInstances {

  val dataType: Data => Type = {
    case Data.Null => Null
    case Data.Str(_) => Str
    case Data.Bool(_) => Bool
    case Data.Dec(_) => Dec
    case Data.Int(_) => Int
    case Data.Obj(v) => Obj(v.mapValues(Const(_)), None)
    case Data.Arr(v) => Arr(v.map(Const(_)))
    case Data.OffsetDateTime(_) => OffsetDateTime
    case Data.OffsetTime(_) => OffsetTime
    case Data.OffsetDate(_) => OffsetDate
    case Data.LocalDateTime(_) => LocalDateTime
    case Data.LocalTime(_) => LocalTime
    case Data.LocalDate(_) => LocalDate
    case Data.Interval(_) => Interval
    case Data.NA => Bottom
  }

  final case object Top               extends Type
  final case object Bottom            extends Type

  final case class Const(value: Data) extends Type

  final case object Null              extends Type
  final case object Str               extends Type
  final case object Int               extends Type
  final case object Dec               extends Type
  final case object Bool              extends Type
  final case object OffsetDateTime    extends Type
  final case object OffsetTime        extends Type
  final case object OffsetDate        extends Type
  final case object LocalDateTime     extends Type
  final case object LocalTime         extends Type
  final case object LocalDate         extends Type
  final case object Interval          extends Type

  final case class Arr(value: List[Type]) extends Type
  final case class FlexArr(minSize: Int, maxSize: Option[Int], value: Type)
      extends Type

  // NB: `unknowns` represents the type of any values where we don’t know the
  //      keys. None means the Obj is fully known.
  final case class Obj(value: Map[String, Type], unknowns: Option[Type])
      extends Type

  final case class Coproduct(left: Type, right: Type) extends Type {
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def flatten: NonEmptyList[Type] = {
      def flatten0(v: Type): NonEmptyList[Type] = v match {
        case left ⨿ right => flatten0(left) append flatten0(right)
        case x            => NonEmptyList(x)
      }

      flatten0(this)
    }

    override def hashCode = flatten.toSet.hashCode()

    @SuppressWarnings(Array("org.wartremover.warts.Equals"))
    override def equals(that: Any) = that match {
      case that @ Coproduct(_, _) =>
        this.flatten.toSet.equals(that.flatten.toSet)
      case _ => false
    }
  }

  object ⨿ {
    def unapply(obj: Type): Option[(Type, Type)] = obj match {
      case Coproduct(a, b) => (a, b).some
      case _               => None
    }
  }

  val AnyArray = FlexArr(0, None, Top)
  val AnyObject = Obj(Map(), Some(Top))
  val Numeric = Int ⨿ Dec
}

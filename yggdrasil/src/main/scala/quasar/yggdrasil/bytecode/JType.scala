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

package quasar.yggdrasil.bytecode

import quasar.Type

sealed trait JType {
  def |(jtype: JType) = JUnionT(this, jtype)
}

sealed trait JPrimitiveType extends JType

case object JNumberT  extends JPrimitiveType
case object JTextT    extends JPrimitiveType
case object JBooleanT extends JPrimitiveType
case object JNullT    extends JPrimitiveType

case object JOffsetDateTimeT extends JPrimitiveType
case object JOffsetTimeT     extends JPrimitiveType
case object JOffsetDateT     extends JPrimitiveType
case object JLocalDateTimeT  extends JPrimitiveType
case object JLocalDateT      extends JPrimitiveType
case object JLocalTimeT      extends JPrimitiveType
case object JIntervalT       extends JPrimitiveType

sealed trait JArrayT extends JType
case class JArrayHomogeneousT(jType: JType)        extends JArrayT
case class JArrayFixedT(elements: Map[Int, JType]) extends JArrayT
case object JArrayUnfixedT extends JArrayT

sealed trait JObjectT extends JType
case class JObjectFixedT(fields: Map[String, JType]) extends JObjectT
case object JObjectUnfixedT extends JObjectT

case class JUnionT(left: JType, right: JType) extends JType {
  override def toString = {
    if (this == JType.JUniverseT)
      "JUniverseT"
    else
      "JUnionT" + "(" + left + ", " + right + ")"
  }
}

object JType {
  val JPrimitiveUnfixedT =
    JNumberT | JTextT | JBooleanT | JNullT | JIntervalT |
      JLocalDateTimeT | JLocalTimeT | JLocalDateT |
      JOffsetDateTimeT | JOffsetTimeT | JOffsetDateT
  val JUniverseT = JPrimitiveUnfixedT | JObjectUnfixedT | JArrayUnfixedT
  val JTemporalT =
    JIntervalT |
      JLocalDateTimeT | JLocalTimeT | JLocalDateT |
      JOffsetDateTimeT | JOffsetTimeT | JOffsetDateT
  val JTemporalAbsoluteT =
    JLocalDateTimeT | JLocalTimeT | JLocalDateT |
      JOffsetDateTimeT | JOffsetTimeT | JOffsetDateT
  val JAbsoluteT = JTemporalAbsoluteT | JNumberT
  val JRelativeT = JAbsoluteT | JIntervalT
  val JDateTimeT =
    JLocalDateTimeT | JOffsetDateTimeT
  val JTimeT =
    JLocalDateTimeT | JLocalTimeT |
      JOffsetDateTimeT | JOffsetTimeT
  val JDateT =
    JLocalDateTimeT | JLocalDateT |
      JOffsetDateTimeT | JOffsetDateT
  val JOffsetT =
    JOffsetDateTimeT | JOffsetTimeT | JOffsetDateT

  // this must be consistent with JValue.fromData
  def fromType(tpe: Type): JType = {
    tpe match {
      case Type.Null => JNullT
      case Type.Str => JTextT
      case Type.Int | Type.Dec => JNumberT
      case Type.Bool => JBooleanT
      case Type.OffsetDateTime => JOffsetDateTimeT | JTextT
      case Type.OffsetTime => JOffsetTimeT | JTextT
      case Type.OffsetDate => JOffsetDateT | JTextT
      case Type.LocalDateTime => JLocalDateTimeT | JTextT
      case Type.LocalTime => JLocalTimeT | JTextT
      case Type.LocalDate => JLocalDateT | JTextT
      case Type.Interval => JIntervalT | JTextT

      case Type.Arr(tpes) =>
        val mapped: Map[Int, JType] =
          tpes.map(fromType).zipWithIndex.map(_.swap)(collection.breakOut)

        JArrayFixedT(mapped)

      case Type.FlexArr(_, _, _) => JArrayUnfixedT

      case Type.Obj(tpes, unknowns) =>
        if (unknowns.isEmpty) {
          val mapped: Map[String, JType] = tpes map {
            case (field, tpe) => field -> fromType(tpe)
          }

          JObjectFixedT(mapped)
        } else {
          JObjectUnfixedT
        }

      case Type.Coproduct(left, right) => fromType(left) | fromType(right)

      case Type.Top => JUniverseT
      case Type.Bottom => JUniverseT

      case Type.Const(_) => JUniverseT
    }
  }
}

case class UnaryOperationType(arg: JType, result: JType)
case class BinaryOperationType(arg0: JType, arg1: JType, result: JType)

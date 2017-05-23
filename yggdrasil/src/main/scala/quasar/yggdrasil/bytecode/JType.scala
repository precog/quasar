/*
 * Copyright 2014–2017 SlamData Inc.
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

sealed trait JType {
  def |(jtype: JType) = JUnionT(this, jtype)
}

sealed trait JPrimitiveType extends JType

case object JNumberT  extends JPrimitiveType
case object JTextT    extends JPrimitiveType
case object JBooleanT extends JPrimitiveType
case object JNullT    extends JPrimitiveType

case object JDateT   extends JPrimitiveType
case object JPeriodT extends JPrimitiveType

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
  val JPrimitiveUnfixedT = JNumberT | JTextT | JBooleanT | JNullT | JDateT | JPeriodT
  val JUniverseT         = JPrimitiveUnfixedT | JObjectUnfixedT | JArrayUnfixedT
}

case class UnaryOperationType(arg: JType, result: JType)
case class BinaryOperationType(arg0: JType, arg1: JType, result: JType)

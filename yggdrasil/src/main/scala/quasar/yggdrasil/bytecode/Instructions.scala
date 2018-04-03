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

trait Instructions {
  type Lib <: Library
  val library: Lib
  object instructions extends InstructionSet[library.type](library)
}

class InstructionSet[Lib <: Library](val library: Lib) {
  import library._

  private def BinOpType(tp: JType) = BinaryOperationType(tp, tp, tp)
  import JType.JUniverseT

  sealed abstract class UnaryOperation(val tpe: UnaryOperationType)
  sealed abstract class BinaryOperation(val tpe: BinaryOperationType)
  sealed abstract class NumericBinaryOperation     extends BinaryOperation(BinOpType(JNumberT))
  sealed abstract class NumericComparisonOperation extends BinaryOperation(BinaryOperationType(JNumberT, JNumberT, JBooleanT))
  sealed abstract class BooleanBinaryOperation     extends BinaryOperation(BinOpType(JBooleanT))
  sealed abstract class EqualityOperation          extends BinaryOperation(BinaryOperationType(JUniverseT, JUniverseT, JBooleanT))

  final case class BuiltInFunction1Op(op: Op1)      extends UnaryOperation(op.tpe)
  final case class BuiltInFunction2Op(op: Op2)      extends BinaryOperation(op.tpe)
  final case class BuiltInMorphism1(mor: Morphism1) extends UnaryOperation(mor.tpe)
  final case class BuiltInMorphism2(mor: Morphism2) extends BinaryOperation(mor.tpe)
  final case class BuiltInReduction(red: Reduction) extends UnaryOperation(red.tpe)

  final case object Add           extends NumericBinaryOperation
  final case object And           extends BooleanBinaryOperation
  final case object ArraySwap     extends BinaryOperation(BinaryOperationType(JArrayUnfixedT, JNumberT, JArrayUnfixedT))
  final case object Comp          extends UnaryOperation(UnaryOperationType(JBooleanT, JBooleanT))
  final case object DerefArray    extends BinaryOperation(BinaryOperationType(JArrayUnfixedT, JNumberT, JUniverseT))
  final case object DerefMetadata extends BinaryOperation(BinOpType(JUniverseT))
  final case object DerefObject   extends BinaryOperation(BinaryOperationType(JObjectUnfixedT, JTextT, JUniverseT))
  final case object Div           extends NumericBinaryOperation
  final case object Eq            extends EqualityOperation
  final case object Gt            extends NumericComparisonOperation
  final case object GtEq          extends NumericComparisonOperation
  final case object JoinArray     extends BinaryOperation(BinOpType(JArrayUnfixedT))
  final case object JoinObject    extends BinaryOperation(BinOpType(JObjectUnfixedT))
  final case object Lt            extends NumericComparisonOperation
  final case object LtEq          extends NumericComparisonOperation
  final case object Mod           extends NumericBinaryOperation
  final case object Mul           extends NumericBinaryOperation
  final case object Neg           extends UnaryOperation(UnaryOperationType(JNumberT, JNumberT))
  final case object New           extends UnaryOperation(UnaryOperationType(JUniverseT, JUniverseT))
  final case object NotEq         extends EqualityOperation
  final case object Or            extends BooleanBinaryOperation
  final case object Pow           extends NumericBinaryOperation
  final case object Sub           extends NumericBinaryOperation
  final case object WrapArray     extends UnaryOperation(UnaryOperationType(JUniverseT, JArrayUnfixedT))
  final case object WrapObject    extends BinaryOperation(BinaryOperationType(JTextT, JUniverseT, JObjectUnfixedT))
}

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

trait Instructions {
  type Lib <: Library
  val library: Lib
  object instructions extends InstructionSet[library.type](library)
}

class InstructionSet[Lib <: Library](val library: Lib) {
  import library._

  sealed trait Instruction

  object RootInstr {
    def unapply(in: Instruction): Boolean = in match {
      case _: PushString => true
      case _: PushNum    => true
      case PushTrue      => true
      case PushFalse     => true
      case PushNull      => true
      case PushObject    => true
      case PushArray     => true
      case _             => false
    }
  }

  sealed trait JoinInstr                          extends Instruction
  final case class Map2Cross(op: BinaryOperation) extends JoinInstr
  final case class Map2Match(op: BinaryOperation) extends JoinInstr
  final case object Assert                        extends JoinInstr
  final case object IIntersect                    extends JoinInstr
  final case object IUnion                        extends JoinInstr
  final case object Observe                       extends JoinInstr
  final case object SetDifference                 extends JoinInstr

  sealed trait DataInstr                                   extends Instruction
  final case class Line(line: Int, col: Int, text: String) extends DataInstr { override def toString = s"<$line:$col>" }
  final case class PushNum(num: String)                    extends DataInstr
  final case class PushString(str: String)                 extends DataInstr
  final case class Swap(depth: Int)                        extends DataInstr
  final case object FilterCross                            extends DataInstr
  final case object FilterMatch                            extends DataInstr

  final case class Group(id: Int)                extends Instruction
  final case class KeyPart(id: Int)              extends Instruction
  final case class Map1(op: UnaryOperation)      extends Instruction
  final case class MergeBuckets(and: Boolean)    extends Instruction
  final case class Morph1(m1: BuiltInMorphism1)  extends Instruction
  final case class Morph2(m2: BuiltInMorphism2)  extends Instruction
  final case class PushGroup(id: Int)            extends Instruction
  final case class PushKey(id: Int)              extends Instruction
  final case class Reduce(red: BuiltInReduction) extends Instruction
  final case object AbsoluteLoad                 extends Instruction
  final case object Distinct                     extends Instruction
  final case object Drop                         extends Instruction
  final case object Dup                          extends Instruction
  final case object Extra                        extends Instruction
  final case object Merge                        extends Instruction
  final case object PushArray                    extends Instruction
  final case object PushFalse                    extends Instruction
  final case object PushNull                     extends Instruction
  final case object PushObject                   extends Instruction
  final case object PushTrue                     extends Instruction
  final case object PushUndefined                extends Instruction
  final case object RelativeLoad                 extends Instruction
  final case object Split                        extends Instruction

  private def DateNumUnion         = JUnionT(JNumberT, JDateT)
  private def BinOpType(tp: JType) = BinaryOperationType(tp, tp, tp)
  import JType.JUniverseT

  sealed abstract class UnaryOperation(val tpe: UnaryOperationType)
  sealed abstract class BinaryOperation(val tpe: BinaryOperationType)
  sealed abstract class NumericBinaryOperation     extends BinaryOperation(BinOpType(JNumberT))
  sealed abstract class NumericComparisonOperation extends BinaryOperation(BinaryOperationType(DateNumUnion, DateNumUnion, JBooleanT))
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

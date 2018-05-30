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

package quasar.mimir

import quasar.yggdrasil._
import quasar.yggdrasil.bytecode._

trait OpFinderModule extends Instructions with TableModule with TableLibModule {
  import instructions._

  trait OpFinder {
    def op1ForUnOp(op: UnaryOperation): library.Op1
    def op2ForBinOp(op: BinaryOperation): Option[library.Op2]
  }
}

trait StdLibOpFinderModule extends Instructions with StdLibModule with OpFinderModule {
  import instructions._
  import library._

  trait StdLibOpFinder extends OpFinder {
    override def op1ForUnOp(op: UnaryOperation) = op match {
      case BuiltInFunction1Op(op1) => op1
      case New | WrapArray         => sys.error("assertion error")
      case Comp                    => Unary.Comp
      case Neg                     => Unary.Neg
      case _                       => sys.error(s"Unexpected op $op")
    }

    override def op2ForBinOp(op: BinaryOperation) = {
      import instructions._

      op match {
        case BuiltInFunction2Op(op2)                                                                    => Some(op2)
        case Add                                                                                        => Some(Infix.Add)
        case Sub                                                                                        => Some(Infix.Sub)
        case Mul                                                                                        => Some(Infix.Mul)
        case Div                                                                                        => Some(Infix.Div)
        case Mod                                                                                        => Some(Infix.Mod)
        case Pow                                                                                        => Some(Infix.Pow)
        case Lt                                                                                         => Some(Infix.Lt)
        case LtEq                                                                                       => Some(Infix.LtEq)
        case Gt                                                                                         => Some(Infix.Gt)
        case GtEq                                                                                       => Some(Infix.GtEq)
        case Eq | instructions.NotEq                                                                    => None
        case WrapObject | JoinObject | JoinArray | ArraySwap | DerefMetadata | DerefObject | DerefArray => None
        case _                                                                                          => sys.error(s"Unexpected op $op")
      }
    }
  }
}

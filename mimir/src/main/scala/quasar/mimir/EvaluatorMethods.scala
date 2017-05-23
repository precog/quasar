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

package quasar.mimir

import quasar.blueeyes._
import quasar.precog.common._
import quasar.yggdrasil._
import quasar.yggdrasil.TableModule.paths
import quasar.yggdrasil.execution.EvaluationContext

trait EvaluatorMethodsModule[M[+ _]] extends DAG with TableModule[M] with TableLibModule[M] with OpFinderModule[M] {
  import dag._
  import instructions._
  import trans._

  trait EvaluatorMethods extends OpFinder {
    def MorphContext(ctx: EvaluationContext, node: DepGraph): MorphContext

    def rValueToCValue(rvalue: RValue): Option[CValue] = rvalue match {
      case cvalue: CValue => Some(cvalue)
      case RArray.empty   => Some(CEmptyArray)
      case RObject.empty  => Some(CEmptyObject)
      case _              => None
    }

    def transRValue[A <: SourceType](rvalue: RValue, target: TransSpec[A]): TransSpec[A] = {
      rValueToCValue(rvalue) map { cvalue =>
        trans.ConstLiteral(cvalue, target)
      } getOrElse {
        rvalue match {
          case RArray(elements) =>
            InnerArrayConcat(elements map { element =>
              trans.WrapArray(transRValue(element, target))
            }: _*)
          case RObject(fields) =>
            InnerObjectConcat(fields.toSeq map {
              case (key, value) => trans.WrapObject(transRValue(value, target), key)
            }: _*)
          case _ =>
            sys.error("Can't handle RValue")
        }
      }
    }

    def transFromBinOp[A <: SourceType](op: BinaryOperation, ctx: MorphContext)(left: TransSpec[A], right: TransSpec[A]): TransSpec[A] = op match {
      case Eq                      => trans.Equal[A](left, right)
      case NotEq                   => op1ForUnOp(Comp).spec(ctx)(trans.Equal[A](left, right))
      case instructions.WrapObject => WrapObjectDynamic(left, right)
      case JoinObject              => InnerObjectConcat(left, right)
      case JoinArray               => InnerArrayConcat(left, right)
      case instructions.ArraySwap  => sys.error("nothing happens")
      case DerefObject             => DerefObjectDynamic(left, right)
      case DerefMetadata           => sys.error("cannot do a dynamic metadata deref")
      case DerefArray              => DerefArrayDynamic(left, right)
      case _                       => op2ForBinOp(op).get.spec(ctx)(left, right)
    }

    def combineTransSpecs(specs: List[TransSpec1]): TransSpec1 =
      specs map { trans.WrapArray(_): TransSpec1 } reduceLeftOption { trans.OuterArrayConcat(_, _) } get

    def buildWrappedJoinSpec(idMatch: IdentityMatch, valueKeys: Set[Int] = Set.empty)(spec: (TransSpec2, TransSpec2) => TransSpec2): TransSpec2 = {
      val leftIdentitySpec  = DerefObjectStatic(Leaf(SourceLeft), paths.Key)
      val rightIdentitySpec = DerefObjectStatic(Leaf(SourceRight), paths.Key)

      val sharedDerefs = for ((i, _) <- idMatch.sharedIndices)
        yield trans.WrapArray(DerefArrayStatic(leftIdentitySpec, CPathIndex(i)))

      val unsharedLeft = for (i <- idMatch.leftIndices)
        yield trans.WrapArray(DerefArrayStatic(leftIdentitySpec, CPathIndex(i)))

      val unsharedRight = for (i <- idMatch.rightIndices)
        yield trans.WrapArray(DerefArrayStatic(rightIdentitySpec, CPathIndex(i)))

      val derefs: Seq[TransSpec2] = sharedDerefs ++ unsharedLeft ++ unsharedRight

      val newIdentitySpec =
        if (derefs.isEmpty)
          trans.ConstLiteral(CEmptyArray, Leaf(SourceLeft))
        else
          derefs reduceLeft { trans.InnerArrayConcat(_, _) }

      val wrappedIdentitySpec = trans.WrapObject(newIdentitySpec, paths.Key.name)

      val leftValueSpec  = DerefObjectStatic(Leaf(SourceLeft), paths.Value)
      val rightValueSpec = DerefObjectStatic(Leaf(SourceRight), paths.Value)

      val wrappedValueSpec = trans.WrapObject(spec(leftValueSpec, rightValueSpec), paths.Value.name)

      val valueKeySpecs = valueKeys map { key =>
        trans.WrapObject(DerefObjectStatic(Leaf(SourceLeft), CPathField("sort-" + key)), "sort-" + key)
      }

      val keyValueSpec = InnerObjectConcat(wrappedValueSpec, wrappedIdentitySpec)

      if (valueKeySpecs.isEmpty) {
        keyValueSpec
      } else {
        InnerObjectConcat(keyValueSpec, OuterObjectConcat(valueKeySpecs.toList: _*))
      }
    }

    def buildWrappedCrossSpec(spec: (TransSpec2, TransSpec2) => TransSpec2): TransSpec2 = {
      val leftIdentitySpec  = DerefObjectStatic(Leaf(SourceLeft), paths.Key)
      val rightIdentitySpec = DerefObjectStatic(Leaf(SourceRight), paths.Key)

      val newIdentitySpec = InnerArrayConcat(leftIdentitySpec, rightIdentitySpec)

      val wrappedIdentitySpec = trans.WrapObject(newIdentitySpec, paths.Key.name)

      val leftValueSpec  = DerefObjectStatic(Leaf(SourceLeft), paths.Value)
      val rightValueSpec = DerefObjectStatic(Leaf(SourceRight), paths.Value)

      val valueSpec        = spec(leftValueSpec, rightValueSpec)
      val wrappedValueSpec = trans.WrapObject(valueSpec, paths.Value.name)

      InnerObjectConcat(wrappedIdentitySpec, wrappedValueSpec)
    }
  }
}

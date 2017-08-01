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

import quasar.precog.common._
import quasar.yggdrasil.execution.EvaluationContext

import Function._

trait StaticInlinerModule[M[+ _]] extends DAG with EvaluatorMethodsModule[M] {
  trait StaticInliner extends EvaluatorMethods {
    def inlineStatics(graph: DepGraph, ctx: EvaluationContext): DepGraph
  }
}

trait StdLibStaticInlinerModule[M[+ _]] extends StaticInlinerModule[M] with StdLibModule[M] {
  import dag._
  import instructions._

  trait StdLibStaticInliner extends StaticInliner {
    def inlineStatics(graph: DepGraph, ctx: EvaluationContext): DepGraph = {
      graph mapDown { recurse =>
        {
          case graph @ Operate(op, child) => {
            recurse(child) match {
              case child2 @ Const(CUndefined) => Const(CUndefined)(child2.loc)

              case child2 @ Const(value) => {
                op match {
                  case instructions.WrapArray =>
                    Const(RArray(value :: Nil))(graph.loc)

                  case _ => {
                    val newOp1 = op1ForUnOp(op)
                    newOp1.fold(
                      _ => Operate(op, child2)(graph.loc),
                      newOp1 => {
                        val result = for {
                          // No Op1F1 that can be applied to a complex RValues
                          cvalue <- RValue.toCValue(value)
                          col <- newOp1.f1.apply(cvalue)
                          if col isDefinedAt 0
                        } yield col cValue 0

                        Const(result getOrElse CUndefined)(graph.loc)
                      }
                    )
                  }
                }
              }

              case child2 => Operate(op, child2)(graph.loc)
            }
          }

          case graph @ Cond(pred, left, leftJoin, right, rightJoin) => {
            val pred2  = recurse(pred)
            val left2  = recurse(left)
            val right2 = recurse(right)

            pred2 match {
              case Const(CBoolean(true))  => left2
              case Const(CBoolean(false)) => right2
              case Const(_)               => Undefined(graph.loc)
              case _                      => Cond(pred2, left2, leftJoin, right2, rightJoin)(graph.loc)
            }
          }

          case graph @ IUI(union, left, right) => {
            val left2  = recurse(left)
            val right2 = recurse(right)

            if (left2 == right2)
              left2
            else
              IUI(union, left2, right2)(graph.loc)
          }

          // Array operations
          case graph @ Join(JoinArray, sort @ Cross(_), left, right) =>
            val left2  = recurse(left)
            val right2 = recurse(right)

            (left2, right2) match {
              case (Const(RArray(l)), Const(RArray(r))) =>
                Const(RArray(l ++ r))(graph.loc)
              case _ =>
                Join(JoinArray, sort, left2, right2)(graph.loc)
            }

          case graph @ Join(DerefArray, sort @ Cross(_), left, right) =>
            val left2  = recurse(left)
            val right2 = recurse(right)

            (left2, right2) match {
              case (Const(RArray(l)), Const(CNum(r))) if r >= 0 && r < l.length =>
                Const(l(r.intValue))(graph.loc)
              case _ =>
                Join(DerefArray, sort, left2, right2)(graph.loc)
            }

          case graph @ Join(ArraySwap, sort @ Cross(_), left, right) =>
            val left2  = recurse(left)
            val right2 = recurse(right)

            (left2, right2) match {
              case (Const(RArray(l)), Const(CNum(r))) if r >= 0 && r < l.length =>
                val (beforeIndex, afterIndex) = l.splitAt(r.intValue)
                val (a, d)                    = afterIndex.splitAt(1)
                val (c, b)                    = beforeIndex.splitAt(1)

                Const(RArray(a ++ b ++ c ++ d))(graph.loc)
              case _ =>
                Join(ArraySwap, sort, left2, right2)(graph.loc)
            }

          // Object operations
          case graph @ Join(WrapObject, sort @ Cross(_), left, right) =>
            val left2  = recurse(left)
            val right2 = recurse(right)

            (left2, right2) match {
              case (Const(CString(k)), Const(v)) =>
                Const(RObject(Map(k -> v)))(graph.loc)
              case _ =>
                Join(WrapObject, sort, left2, right2)(graph.loc)
            }

          case graph @ Join(DerefObject, sort @ Cross(_), left, right) =>
            val left2  = recurse(left)
            val right2 = recurse(right)

            (left2, right2) match {
              case (Const(RObject(v)), Const(CString(k))) if v contains k =>
                Const(v(k))(graph.loc)
              case _ =>
                Join(DerefObject, sort, left2, right2)(graph.loc)
            }

          case graph @ Join(JoinObject, sort @ Cross(_), left, right) =>
            val left2  = recurse(left)
            val right2 = recurse(right)

            (left2, right2) match {
              case (Const(RObject(l)), Const(RObject(r))) =>
                Const(RObject(l ++ r))(graph.loc)
              case _ =>
                Join(JoinObject, sort, left2, right2)(graph.loc)
            }

          case graph @ Join(op, sort @ Cross(_), left, right) => {
            val left2  = recurse(left)
            val right2 = recurse(right)

            val graphM = for {
              op2 <- op2ForBinOp(op)
              op2F2 <- op2.fold(op2 = const(None), op2F2 = { Some(_) })
              result <- (left2, right2) match {
                         case (left2 @ Const(CUndefined), _) =>
                           Some(Const(CUndefined)(left2.loc))

                         case (_, right2 @ Const(CUndefined)) =>
                           Some(Const(CUndefined)(right2.loc))

                         case (left2 @ Const(leftValue), right2 @ Const(rightValue)) => {
                           val result = for {
                             // No Op1F1 that can be applied to a complex RValues
                             leftCValue <- RValue.toCValue(leftValue)
                             rightCValue <- RValue.toCValue(rightValue)
                             col <- op2F2.f2.partialLeft(leftCValue).apply(rightCValue)
                             if col isDefinedAt 0
                           } yield col cValue 0

                           Some(Const(result getOrElse CUndefined)(graph.loc))
                         }

                         case _ => None
                       }
            } yield result

            graphM getOrElse Join(op, sort, left2, right2)(graph.loc)
          }

          case Filter(sort @ (IdentitySort | ValueSort(_)), left, right) => {
            val left2  = recurse(left)
            val right2 = recurse(right)

            val back = (left2, right2) match {
              case (_, Const(CUndefined)) => Some(Const(CUndefined)(graph.loc))
              case (Const(CUndefined), _) => Some(Const(CUndefined)(graph.loc))
              case _                      => None
            }

            back getOrElse Filter(sort, left2, right2)(graph.loc)
          }

          case graph @ Filter(sort @ Cross(_), left, right) => {
            val left2  = recurse(left)
            val right2 = recurse(right)

            val back = (left2, right2) match {
              case (_, Const(CUndefined))     => Some(Const(CUndefined)(graph.loc))
              case (Const(CUndefined), _)     => Some(Const(CUndefined)(graph.loc))
              case (_, right2 @ Const(CTrue)) => Some(left2)
              case (_, right2 @ Const(_))     => Some(Const(CUndefined)(graph.loc))
              case _                          => None
            }

            back getOrElse Filter(sort, left2, right2)(graph.loc)
          }
        }
      }
    }
  }
}

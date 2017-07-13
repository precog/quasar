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
import quasar.yggdrasil._
import quasar.yggdrasil.bytecode._
import quasar.yggdrasil.execution.EvaluationContext

import scalaz._, Scalaz._

trait TransSpecableModule[M[+ _]] extends TransSpecModule with TableModule[M] with EvaluatorMethodsModule[M] {
  import dag._
  import library._
  import instructions._

  trait TransSpecable extends EvaluatorMethods {
    import trans._

    trait TransSpecableOrderFold[T] {
      def WrapObject(node: Join)(parent: T, field: String): T
      def DerefObjectStatic(node: Join)(parent: T, field: String): T
      def DerefArrayStatic(node: Join)(parent: T, index: Int): T
      def WrapArray(node: Operate)(parent: T): T
      def unmatched(node: DepGraph): T
      def done(node: DepGraph): T
    }

    trait TransSpecableFold[T] extends TransSpecableOrderFold[T] {
      def EqualLiteral(node: Join)(parent: T, value: RValue, invert: Boolean): T
      def DerefMetadataStatic(node: Join)(parent: T, field: String): T
      def ArraySwap(node: Join)(parent: T, index: Int): T
      def InnerObjectConcat(node: Join)(parent: T): T
      def InnerArrayConcat(node: Join)(parent: T): T
      def Map1Left(node: Join)(parent: T, op: Op2F2, graph: DepGraph, value: RValue): T
      def Map1Right(node: Join)(parent: T, op: Op2F2, graph: DepGraph, value: RValue): T
      def Const(node: dag.Const)(under: T): T
      def binOp(node: Join)(leftParent: T, rightParent: => T, op: BinaryOperation): T
      def Filter(node: dag.Filter)(leftParent: T, rightParent: => T): T
      def Op1(node: Operate)(parent: T, op: UnaryOperation): T
      def Cond(node: dag.Cond)(pred: T, left: T, right: T): T
    }

    def isTransSpecable(to: DepGraph, from: DepGraph): Boolean =
      foldDownTransSpecable(to, Some(from))(new TransSpecableFold[Boolean] {
        def EqualLiteral(node: Join)(parent: Boolean, value: RValue, invert: Boolean)            = parent
        def WrapObject(node: Join)(parent: Boolean, field: String)                               = parent
        def DerefObjectStatic(node: Join)(parent: Boolean, field: String)                        = parent
        def DerefMetadataStatic(node: Join)(parent: Boolean, field: String)                      = parent
        def DerefArrayStatic(node: Join)(parent: Boolean, index: Int)                            = parent
        def ArraySwap(node: Join)(parent: Boolean, index: Int)                                   = parent
        def InnerObjectConcat(node: Join)(parent: Boolean)                                       = parent
        def InnerArrayConcat(node: Join)(parent: Boolean)                                        = parent
        def Map1Left(node: Join)(parent: Boolean, op: Op2F2, graph: DepGraph, value: RValue)     = parent
        def Map1Right(node: Join)(parent: Boolean, op: Op2F2, graph: DepGraph, value: RValue)    = parent
        def binOp(node: Join)(leftParent: Boolean, rightParent: => Boolean, op: BinaryOperation) = leftParent && rightParent
        def Filter(node: dag.Filter)(leftParent: Boolean, rightParent: => Boolean)               = leftParent && rightParent
        def WrapArray(node: Operate)(parent: Boolean)                                            = parent
        def Op1(node: Operate)(parent: Boolean, op: UnaryOperation)                              = parent
        def Cond(node: dag.Cond)(pred: Boolean, left: Boolean, right: Boolean)                   = pred && left && right
        def Const(node: dag.Const)(under: Boolean)                                               = under
        def unmatched(node: DepGraph)                                                            = false
        def done(node: DepGraph)                                                                 = true
      })

    private[this] def snd[A, B](a: A, b: B): Option[B] = Some(b)

    def mkTransSpec(to: DepGraph, from: DepGraph, ctx: EvaluationContext): Option[TransSpec1] =
      mkTransSpecWithState[Option, (TransSpec1, DepGraph)](to, Some(from), ctx, identity, snd, some).map(_._1)

    def findAncestor(to: DepGraph, ctx: EvaluationContext): Option[DepGraph] =
      mkTransSpecWithState[Option, (TransSpec1, DepGraph)](to, None, ctx, identity, snd, some).map(_._2)

    def findTransSpecAndAncestor(to: DepGraph, ctx: EvaluationContext): Option[(TransSpec1, DepGraph)] =
      mkTransSpecWithState[Option, (TransSpec1, DepGraph)](to, None, ctx, identity, snd, some)

    def findOrderAncestor(to: DepGraph, ctx: EvaluationContext): Option[DepGraph] =
      mkTransSpecOrderWithState[Option, (TransSpec1, DepGraph)](to, None, ctx, identity, snd, some).map(_._2)

    def transFold[N[_]: Monad, S](to: DepGraph,
                                    from: Option[DepGraph],
                                    ctx: EvaluationContext,
                                    get: S => (TransSpec1, DepGraph),
                                    set: (S, (TransSpec1, DepGraph)) => N[S],
                                    init: ((TransSpec1, DepGraph)) => N[S]) = {

      // Bifunctor leftMap would be better here if it existed in pimped type inferrable form
      def leftMap(parent: S)(f: TransSpec1 => TransSpec1) = get(parent) match {
        case (spec, ancestor) => set(parent, (f(spec), ancestor))
      }

      new TransSpecableFold[N[S]] {
        import trans._

        def EqualLiteral(node: Join)(parent: N[S], value: RValue, invert: Boolean) =
          parent.flatMap(leftMap(_) { target =>
            val inner = trans.Equal(target, trans.transRValue(value, target))
            if (invert) op1ForUnOp(Comp).spec(inner) else inner
          })

        def WrapObject(node: Join)(parent: N[S], field: String) =
          parent.flatMap(leftMap(_)(trans.WrapObject(_, field)))

        def DerefObjectStatic(node: Join)(parent: N[S], field: String) =
          parent.flatMap(leftMap(_)(trans.DerefObjectStatic(_, CPathField(field))))

        def DerefMetadataStatic(node: Join)(parent: N[S], field: String) =
          parent.flatMap(leftMap(_)(trans.DerefMetadataStatic(_, CPathMeta(field))))

        def DerefArrayStatic(node: Join)(parent: N[S], index: Int) =
          parent.flatMap(leftMap(_)(trans.DerefArrayStatic(_, CPathIndex(index))))

        def ArraySwap(node: Join)(parent: N[S], index: Int) = {
          parent.flatMap(leftMap(_)(trans.ArraySwap(_, index)))
        }

        def InnerObjectConcat(node: Join)(parent: N[S]) =
          parent.flatMap(leftMap(_)(trans.InnerObjectConcat(_)))

        def InnerArrayConcat(node: Join)(parent: N[S]) =
          parent.flatMap(leftMap(_)(trans.InnerArrayConcat(_)))

        def Map1Left(node: Join)(parent: N[S], op: Op2F2, graph: DepGraph, value: RValue) =
          parent.flatMap(leftMap(_) { target =>
            value match {
              case cv: CValue =>
                trans.Map1(target, op.f2.applyr(cv))

              case _ =>
                trans.Typed(trans.Typed(target, JNullT), JTextT) // nuke all the things
            }
          })

        def Map1Right(node: Join)(parent: N[S], op: Op2F2, graph: DepGraph, value: RValue) =
          parent.flatMap(leftMap(_) { target =>
            value match {
              case cv: CValue =>
                trans.Map1(target, op.f2.applyl(cv))

              case _ =>
                trans.Typed(trans.Typed(target, JNullT), JTextT) // nuke all the things
            }
          })

        def binOp(node: Join)(leftParent: N[S], rightParent: => N[S], op: BinaryOperation) = {
          for {
            pl <- leftParent
            (l, al) = get(pl)
            pr <- rightParent
            (r, ar) = get(pr)
            result <- if (al == ar) {
                       set(pl, (transFromBinOp(op)(l, r), al))
                     } else {
                       init(Leaf(Source), node)
                     }
          } yield result
        }

        def Filter(node: dag.Filter)(leftParent: N[S], rightParent: => N[S]) =
          for {
            pl <- leftParent
            (l, al) = get(pl)
            pr <- rightParent
            (r, ar) = get(pr)
            result <- if (al == ar) set(pl, (trans.Filter(l, r), al)) else init(Leaf(Source), node)
          } yield result

        def WrapArray(node: Operate)(parent: N[S]) =
          parent.flatMap(leftMap(_)(trans.WrapArray(_)))

        def Op1(node: Operate)(parent: N[S], op: UnaryOperation) =
          parent.flatMap(leftMap(_)(parent => op1ForUnOp(op).spec(parent)))

        def Cond(node: dag.Cond)(pred: N[S], left: N[S], right: N[S]) = {
          for {
            pp <- pred
            (p, ap) = get(pp)
            pl <- left
            (l, al) = get(pl)
            pr <- right
            (r, ar) = get(pr)

            result <- if (ap == al && al == ar)
                       set(pp, (trans.Cond(p, l, r), ap))
                     else
                       init(Leaf(Source), node)
          } yield result
        }

        def Const(node: dag.Const)(underN: N[S]) = {
          val dag.Const(cv: CValue) = node // TODO !!

          underN flatMap { under =>
            leftMap(under) { spec =>
              trans.ConstLiteral(cv, spec)
            }
          }
        }

        def unmatched(node: DepGraph) = init(Leaf(Source), node)

        def done(node: DepGraph) = init(Leaf(Source), node)
      }
    }

    def mkTransSpecWithState[N[_]: Monad, S](to: DepGraph,
                                               from: Option[DepGraph],
                                               ctx: EvaluationContext,
                                               get: S => (TransSpec1, DepGraph),
                                               set: (S, (TransSpec1, DepGraph)) => N[S],
                                               init: ((TransSpec1, DepGraph)) => N[S]): N[S] = {

      foldDownTransSpecable(to, from)(transFold[N, S](to, from, ctx, get, set, init))
    }

    def mkTransSpecOrderWithState[N[+ _]: Monad, S](to: DepGraph,
                                                    from: Option[DepGraph],
                                                    ctx: EvaluationContext,
                                                    get: S => (TransSpec1, DepGraph),
                                                    set: (S, (TransSpec1, DepGraph)) => N[S],
                                                    init: ((TransSpec1, DepGraph)) => N[S]): N[S] = {

      foldDownTransSpecableOrder(to, from)(transFold[N, S](to, from, ctx, get, set, init))
    }

    object ConstInt {
      def unapply(c: Const) = c match {
        case Const(CNum(n))    => Some(n.toInt)
        case Const(CLong(n))   => Some(n.toInt)
        case Const(CDouble(n)) => Some(n.toInt)
        case _                 => None
      }
    }

    object Op2F2ForBinOp {
      def unapply(op: BinaryOperation): Option[Op2F2] = op2ForBinOp(op).flatMap {
        case op2f2: Op2F2 => Some(op2f2)
        case _            => None
      }
    }

    def foldDownTransSpecableOrder[T](to: DepGraph, from: Option[DepGraph])(alg: TransSpecableOrderFold[T]): T = {

      def loop(graph: DepGraph): T = graph match {
        case node if from.map(_ == node).getOrElse(false) => alg.done(node)

        case node @ Join(instructions.WrapObject, Cross(_), Const(CString(field)), right) =>
          alg.WrapObject(node)(loop(right), field)

        case node @ Join(DerefObject, Cross(_), left, Const(CString(field))) =>
          alg.DerefObjectStatic(node)(loop(left), field)

        case node @ Join(DerefArray, Cross(_), left, ConstInt(index)) =>
          alg.DerefArrayStatic(node)(loop(left), index)

        case node @ Operate(instructions.WrapArray, parent) =>
          alg.WrapArray(node)(loop(parent))

        case node => alg.unmatched(node)
      }

      loop(to)
    }

    def foldDownTransSpecable[T](to: DepGraph, from: Option[DepGraph])(alg: TransSpecableFold[T]): T = {

      def loop(graph: DepGraph): T = graph match {
        case node if from.map(_ == node).getOrElse(false) => alg.done(node)

        case node @ dag.Cond(pred, left @ dag.Const(_: CValue), Cross(_), right, IdentitySort | ValueSort(_)) => {
          val predRes = loop(pred)

          alg.Cond(node)(predRes, alg.Const(left)(predRes), loop(right))
        }

        case node @ dag.Cond(pred, left, IdentitySort | ValueSort(_), right @ dag.Const(_: CValue), Cross(_)) => {
          val predRes = loop(pred)

          alg.Cond(node)(predRes, loop(left), alg.Const(right)(predRes))
        }

        case node @ dag.Cond(pred, left @ dag.Const(_: CValue), Cross(_), right @ dag.Const(_: CValue), Cross(_)) => {
          val predRes = loop(pred)

          alg.Cond(node)(predRes, alg.Const(left)(predRes), alg.Const(right)(predRes))
        }

        case node @ dag.Cond(pred, left, IdentitySort | ValueSort(_), right, IdentitySort | ValueSort(_)) =>
          alg.Cond(node)(loop(pred), loop(left), loop(right))

        case node @ Join(Eq, Cross(_), left, Const(value)) =>
          alg.EqualLiteral(node)(loop(left), value, false)

        case node @ Join(Eq, Cross(_), Const(value), right) =>
          alg.EqualLiteral(node)(loop(right), value, false)

        case node @ Join(NotEq, Cross(_), left, Const(value)) =>
          alg.EqualLiteral(node)(loop(left), value, true)

        case node @ Join(NotEq, Cross(_), Const(value), right) =>
          alg.EqualLiteral(node)(loop(right), value, true)

        case node @ Join(instructions.WrapObject, Cross(_), Const(CString(field)), right) =>
          alg.WrapObject(node)(loop(right), field)

        case node @ Join(DerefObject, Cross(_), left, Const(CString(field))) =>
          alg.DerefObjectStatic(node)(loop(left), field)

        case node @ Join(DerefMetadata, Cross(_), left, Const(CString(field))) =>
          alg.DerefMetadataStatic(node)(loop(left), field)

        case node @ Join(DerefArray, Cross(_), left, ConstInt(index)) =>
          alg.DerefArrayStatic(node)(loop(left), index)

        case node @ Join(instructions.ArraySwap, Cross(_), left, ConstInt(index)) =>
          alg.ArraySwap(node)(loop(left), index)

        case node @ Join(JoinObject, Cross(_), left, Const(RObject.empty)) =>
          alg.InnerObjectConcat(node)(loop(left))

        case node @ Join(JoinObject, Cross(_), Const(RObject.empty), right) =>
          alg.InnerObjectConcat(node)(loop(right))

        case node @ Join(JoinArray, Cross(_), left, Const(RArray.empty)) =>
          alg.InnerArrayConcat(node)(loop(left))

        case node @ Join(JoinArray, Cross(_), Const(RArray.empty), right) =>
          alg.InnerArrayConcat(node)(loop(right))

        case node @ Join(Op2F2ForBinOp(op), Cross(_), left, Const(value)) =>
          alg.Map1Left(node)(loop(left), op, left, value)

        case node @ Join(Op2F2ForBinOp(op), Cross(_), Const(value), right) =>
          alg.Map1Right(node)(loop(right), op, right, value)

        case node @ Join(op, joinSort @ (IdentitySort | ValueSort(_)), left, right) =>
          alg.binOp(node)(loop(left), loop(right), op)

        case node @ dag.Filter(joinSort @ (IdentitySort | ValueSort(_)), left, right) =>
          alg.Filter(node)(loop(left), loop(right))

        case node @ Operate(instructions.WrapArray, parent) =>
          alg.WrapArray(node)(loop(parent))

        case node @ Operate(op, parent) =>
          alg.Op1(node)(loop(parent), op)

        case node => alg.unmatched(node)
      }

      loop(to)
    }
  }
}

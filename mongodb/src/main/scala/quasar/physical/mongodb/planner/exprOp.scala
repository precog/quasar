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

package quasar.physical.mongodb.planner

import slamdata.Predef._
import quasar.fp.ski._
import quasar.contrib.iota.copkTraverse
import quasar.physical.mongodb.Bson
import quasar.physical.mongodb.accumulator._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.planner.common._
import quasar.qscript._

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._

object exprOp {
  def processMapFuncExpr
    [T[_[_]]: BirecursiveT: ShowT, M[_]: Monad, EX[_]: Traverse, A]
    (funcHandler: AlgebraM[M, MapFunc[T, ?], Fix[EX]], staticHandler: StaticHandler[T, EX])
    (recovery: A => Fix[EX])
    (fm: FreeMapA[T, A])
    (implicit inj: EX :<: ExprOp)
      : M[Fix[ExprOp]] = {

    val alg: AlgebraM[M, CoEnvMapA[T, A, ?], Fix[EX]] =
      interpretM[M, MapFunc[T, ?], A, Fix[EX]](
        recovery(_).point[M],
        funcHandler)

    def convert(e: EX[FreeMapA[T, A]]): M[Fix[EX]] =
      e.traverse(_.cataM(alg)).map(_.embed)

    val expr: M[Fix[EX]] = staticHandler.handle(fm).map(convert) getOrElse fm.cataM(alg)

    expr.map(_.transCata[Fix[ExprOp]](inj))
  }

  // TODO: Should have a JsFn version of this for $reduce nodes.
  val accumulator: ReduceFunc[Fix[ExprOp]] => AccumOp[Fix[ExprOp]] = {
    import quasar.qscript.ReduceFuncs._
    import fixExprOp._

    {
      case Arbitrary(a)     => $first(a)
      case First(a)         => $first(a)
      case Last(a)          => $last(a)
      case Avg(a)           => $avg(a)
      case Count(_)         => $sum($literal(Bson.Int32(1)))
      case Max(a)           => $max(a)
      case Min(a)           => $min(a)
      case Sum(a)           => $sum(a)
      case UnshiftArray(a)  => $push(a)
      case UnshiftMap(k, v) => ???
    }
  }

  def getExpr[
    T[_[_]]: BirecursiveT: ShowT,
    M[_]: Monad, EX[_]: Traverse: Inject[?[_], ExprOp]]
    (funcHandler: AlgebraM[M, MapFunc[T, ?], Fix[EX]], staticHandler: StaticHandler[T, EX])
    (implicit EX: ExprOpCoreF :<: EX)
      : FreeMap[T] => M[Fix[ExprOp]] =
    processMapFuncExpr[T, M, EX, Hole](funcHandler, staticHandler)(κ(fixExprOpCore[EX].$$ROOT))

  def getExprMerge[T[_[_]]: BirecursiveT: ShowT, M[_]: Monad, EX[_]: Traverse]
    (funcHandler: AlgebraM[M, MapFunc[T, ?], Fix[EX]], staticHandler: StaticHandler[T, EX])
    (a1: DocVar, a2: DocVar)
    (implicit EX: ExprOpCoreF :<: EX, inj: EX :<: ExprOp)
      : JoinFunc[T] => M[Fix[ExprOp]] =
    processMapFuncExpr[T, M, EX, JoinSide](funcHandler, staticHandler) {
      case LeftSide => fixExprOpCore[EX].$var(a1)
      case RightSide => fixExprOpCore[EX].$var(a2)
    }

  def getExprRed[T[_[_]]: BirecursiveT: ShowT, M[_]: Monad, EX[_]: Traverse]
    (funcHandler: AlgebraM[M, MapFunc[T, ?], Fix[EX]], staticHandler: StaticHandler[T, EX])
    (implicit EX: ExprOpCoreF :<: EX, ev: EX :<: ExprOp)
      : FreeMapA[T, ReduceIndex] => M[Fix[ExprOp]] =
    processMapFuncExpr[T, M, EX, ReduceIndex](funcHandler, staticHandler)(_.idx.fold(
      i => fixExprOpCore[EX].$field("_id", i.toString),
      i => fixExprOpCore[EX].$field(createFieldName("f", i))))
}

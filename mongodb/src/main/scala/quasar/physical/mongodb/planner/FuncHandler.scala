/*
 * Copyright 2014–2016 SlamData Inc.
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

import quasar.Predef._
import quasar.physical.mongodb.Bson
import quasar.physical.mongodb.expression._
import quasar.qscript._, MapFuncs._

import org.threeten.bp.Instant
import matryoshka._
import scalaz.{Divide => _, _}, Scalaz._

final case class FuncHandler[T[_[_]], F[_]](run: MapFunc[T, ?] ~> λ[α => Option[Free[F, α]]]) { self =>

  def orElse[G[_], H[_]](other: FuncHandler[T, G])
      (implicit injF: F :<: H, injG: G :<: H): FuncHandler[T, H] =
    new FuncHandler[T, H](λ[MapFunc[T, ?] ~> λ[α => Option[Free[H, α]]]](f =>
      self.run(f).map(_.mapSuspension(injF)) orElse
      other.run(f).map(_.mapSuspension(injG))))
    }

object FuncHandler {
  type M[F[_], A] = Option[Free[F, A]]

  def handleOpsCore[T[_[_]]]: FuncHandler[T, ExprOpCoreF] = {
    def hole[D](d: D): Free[ExprOpCoreF, D] = Free.pure(d)

    new FuncHandler[T, ExprOpCoreF](new (MapFunc[T, ?] ~> M[ExprOpCoreF, ?]) {
      def apply[A](fa: MapFunc[T, A]): M[ExprOpCoreF, A] = {
        val fp = ExprOpCoreF.fixpoint[Free[?[_], A], ExprOpCoreF]
        import fp._

        fa.some collect {
          case Add(a1, a2)           => $add(hole(a1), hole(a2))
          case Multiply(a1, a2)      => $multiply(hole(a1), hole(a2))
          case Subtract(a1, a2)      => $subtract(hole(a1), hole(a2))
          case Divide(a1, a2)        => $divide(hole(a1), hole(a2))
          case Modulo(a1, a2)        => $mod(hole(a1), hole(a2))
          case Negate(a1)            => $multiply($literal(Bson.Int32(-1)), hole(a1))

          case Eq(a1, a2)            => $eq(hole(a1), hole(a2))
          case Neq(a1, a2)           => $neq(hole(a1), hole(a2))
          case Lt(a1, a2)            => $lt(hole(a1), hole(a2))
          case Lte(a1, a2)           => $lte(hole(a1), hole(a2))
          case Gt(a1, a2)            => $gt(hole(a1), hole(a2))
          case Gte(a1, a2)           => $gte(hole(a1), hole(a2))

          case Coalesce(a1, a2)      => $ifNull(hole(a1), hole(a2))

          case ConcatArrays(a1, a2)  => $concat(hole(a1), hole(a2))  // NB: this is valid for strings only
          case Lower(a1)             => $toLower(hole(a1))
          case Upper(a1)             => $toUpper(hole(a1))
          case Substring(a1, a2, a3) => $substr(hole(a1), hole(a2), hole(a3))

          case Cond(a1, a2, a3)      => $cond(hole(a1), hole(a2), hole(a3))

          case Or(a1, a2)            => $or(hole(a1), hole(a2))
          case And(a1, a2)           => $and(hole(a1), hole(a2))
          case Not(a1)               => $not(hole(a1))

          case Null(a1) =>
            $cond($eq(hole(a1), $literal(Bson.Text("null"))),
              $literal(Bson.Null),
              $literal(Bson.Undefined))

          case Bool(a1) =>
            $cond($eq(hole(a1), $literal(Bson.Text("true"))),
              $literal(Bson.Bool(true)),
              $cond($eq(hole(a1), $literal(Bson.Text("false"))),
                $literal(Bson.Bool(false)),
                $literal(Bson.Undefined)))

          case ToTimestamp(a1) =>
            $add($literal(Bson.Date(Instant.ofEpochMilli(0))), hole(a1))

          case Between(a1, a2, a3)   => $and($lte(hole(a2), hole(a1)),
                                              $lte(hole(a1), hole(a3)))
        }
      }
    })
  }

  def handleOps3_0[T[_[_]]]: FuncHandler[T, ExprOp3_0F] = {
    def hole[D](d: D): Free[ExprOp3_0F, D] = Free.pure(d)
    new FuncHandler[T, ExprOp3_0F](new (MapFunc[T, ?] ~> M[ExprOp3_0F, ?]) {
      def apply[A](fa: MapFunc[T, A]): M[ExprOp3_0F, A] = {
        val fp = ExprOp3_0F.fixpoint[Free[?[_], A], ExprOp3_0F]
        import fp._
        import FormatSpecifier._

        fa.some collect {
          case TimeOfDay(a1) =>
            $dateToString(Hour :: ":" :: Minute :: ":" :: Second :: "." :: Millisecond :: FormatString.empty, hole(a1))
        }
      }
    })
  }

  def handleOps3_2[T[_[_]]]: FuncHandler[T, ExprOp3_2F] = {
    new FuncHandler[T, ExprOp3_2F](new (MapFunc[T, ?] ~> M[ExprOp3_2F, ?]) {
      def apply[A](fa: MapFunc[T, A]): M[ExprOp3_2F, A] = {
        None
      }
    })
  }

  def handle2_6[T[_[_]]]: FuncHandler[T, Expr2_6] = handleOpsCore
  def handle3_0[T[_[_]]]: FuncHandler[T, Expr3_0] = handleOps3_0 orElse handle2_6
  def handle3_2[T[_[_]]]: FuncHandler[T, Expr3_2] = handleOps3_2 orElse handle3_0
}

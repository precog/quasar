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
import quasar.qscript.{Coalesce => _, _}, MapFuncs._

import java.time.Instant
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

  def handleOpsCore[T[_[_]], EX[_]: Functor](trunc: Free[EX, ?] ~> Free[EX, ?])(implicit inj: ExprOpCoreF :<: EX): FuncHandler[T, EX] = {
    implicit def hole[D](d: D): Free[EX, D] = Free.pure(d)

    new FuncHandler[T, EX](new (MapFunc[T, ?] ~> M[EX, ?]) {
      def apply[A](fa: MapFunc[T, A]): M[EX, A] = {
        val fp = ExprOpCoreF.fixpoint[Free[?[_], A], EX]
        import fp._

        fa.some collect {
          case Undefined()           => $literal(Bson.Undefined)
          case Add(a1, a2)           => $add(a1, a2)
          case Multiply(a1, a2)      => $multiply(a1, a2)
          case Subtract(a1, a2)      => $subtract(a1, a2)
          case Divide(a1, a2)        => $divide(a1, a2)
          case Modulo(a1, a2)        => $mod(a1, a2)
          case Negate(a1)            => $multiply($literal(Bson.Int32(-1)), a1)

          case MapFuncs.Eq(a1, a2)   => $eq(a1, a2)
          case Neq(a1, a2)           => $neq(a1, a2)
          case Lt(a1, a2)            => $lt(a1, a2)
          case Lte(a1, a2)           => $lte(a1, a2)
          case Gt(a1, a2)            => $gt(a1, a2)
          case Gte(a1, a2)           => $gte(a1, a2)

          case ConcatArrays(a1, a2)  => $concat(a1, a2)  // NB: this is valid for strings only
          case Lower(a1)             => $toLower(a1)
          case Upper(a1)             => $toUpper(a1)
          case Substring(a1, a2, a3) => $substr(a1, a2, a3)

          case Cond(a1, a2, a3)      => $cond(a1, a2, a3)

          case Or(a1, a2)            => $or(a1, a2)
          case And(a1, a2)           => $and(a1, a2)
          case Not(a1)               => $not(a1)

          case Null(a1) =>
            $cond($eq(a1, $literal(Bson.Text("null"))),
              $literal(Bson.Null),
              $literal(Bson.Undefined))

          case Bool(a1) =>
            $cond($eq(a1, $literal(Bson.Text("true"))),
              $literal(Bson.Bool(true)),
              $cond($eq(a1, $literal(Bson.Text("false"))),
                $literal(Bson.Bool(false)),
                $literal(Bson.Undefined)))

          case ExtractCentury(a1) =>
            trunc($divide($add($year(a1), $literal(Bson.Int32(99))), $literal(Bson.Int32(100))))
          case ExtractDayOfMonth(a1) => $dayOfMonth(a1)
          case ExtractDecade(a1) => trunc($divide($year(a1), $literal(Bson.Int32(10))))
          case ExtractDayOfWeek(a1) => $add($dayOfWeek(a1), $literal(Bson.Int32(-1)))
          case ExtractDayOfYear(a1) => $dayOfYear(a1)
          case ExtractEpoch(a1) =>
            $divide(
              $subtract(a1, $literal(Bson.Date(Instant.ofEpochMilli(0)))),
              $literal(Bson.Int32(1000)))
          case ExtractHour(a1) => $hour(a1)
          case ExtractIsoDayOfWeek(a1) =>
            $cond($eq($dayOfWeek(a1), $literal(Bson.Int32(1))),
              $literal(Bson.Int32(7)),
              $add($dayOfWeek(a1), $literal(Bson.Int32(-1))))
          // TODO: case ExtractIsoYear(a1) =>
          case ExtractMicroseconds(a1) =>
            $multiply(
              $add(
                $multiply($second(a1), $literal(Bson.Int32(1000))),
                $millisecond(a1)),
              $literal(Bson.Int32(1000)))
          case ExtractMillennium(a1) =>
            trunc($divide($add($year(a1), $literal(Bson.Int32(999))), $literal(Bson.Int32(1000))))
          case ExtractMilliseconds(a1) =>
            $add(
              $multiply($second(a1), $literal(Bson.Int32(1000))),
              $millisecond(a1))
          case ExtractMinute(a1) => $minute(a1)
          case ExtractMonth(a1) => $month(a1)
          case ExtractQuarter(a1) =>
            trunc(
              $add(
                $divide(
                  $subtract($month(a1), $literal(Bson.Int32(1))),
                  $literal(Bson.Int32(3))),
                $literal(Bson.Int32(1))))
          case ExtractSecond(a1) =>
            $add($second(a1), $divide($millisecond(a1), $literal(Bson.Int32(1000))))
          case ExtractWeek(a1) => $week(a1)
          case ExtractYear(a1) => $year(a1)

          case ToTimestamp(a1) =>
            $add($literal(Bson.Date(Instant.ofEpochMilli(0))), a1)

          case Between(a1, a2, a3)   => $and($lte(a2, a1),
                                              $lte(a1, a3))
        }
      }
    })
  }

  def handleOps3_0[T[_[_]]]: FuncHandler[T, ExprOp3_0F] = {
    implicit def hole[D](d: D): Free[ExprOp3_0F, D] = Free.pure(d)
    new FuncHandler[T, ExprOp3_0F](new (MapFunc[T, ?] ~> M[ExprOp3_0F, ?]) {
      def apply[A](fa: MapFunc[T, A]): M[ExprOp3_0F, A] = {
        val fp = ExprOp3_0F.fixpoint[Free[?[_], A], ExprOp3_0F]
        import fp._
        import FormatSpecifier._

        fa.some collect {
          case TimeOfDay(a1) =>
            $dateToString(Hour :: ":" :: Minute :: ":" :: Second :: "." :: Millisecond :: FormatString.empty, a1)
        }
      }
    })
  }

  def handleOps3_2[T[_[_]]]: FuncHandler[T, ExprOp3_2F] = {
    implicit def hole[D](d: D): Free[ExprOp3_2F, D] = Free.pure(d)
    new FuncHandler[T, ExprOp3_2F](new (MapFunc[T, ?] ~> M[ExprOp3_2F, ?]) {
      def apply[A](fa: MapFunc[T, A]): M[ExprOp3_2F, A] = {
        val fp = ExprOp3_2F.fixpoint[Free[?[_], A], ExprOp3_2F]
        import fp._

        fa.some collect {
          case Power(a1, a2) =>
            $pow(a1, a2)
        }
      }
    })
  }

  def trunc2_6[EX[_]: Functor](implicit inj: ExprOpCoreF :<: EX): Free[EX, ?] ~> Free[EX, ?] =
    new (Free[EX, ?] ~> Free[EX, ?]) {
      def apply[A](expr: Free[EX, A]): Free[EX, A] = {
        val fp = ExprOpCoreF.fixpoint[Free[?[_], A], EX]
        import fp._

        $subtract(expr, $mod(expr, $literal(Bson.Int32(1))))
      }
    }

  def trunc3_2[EX[_]: Functor](implicit inj: ExprOp3_2F :<: EX): Free[EX, ?] ~> Free[EX, ?] =
    new (Free[EX, ?] ~> Free[EX, ?]) {
      def apply[A](expr: Free[EX, A]): Free[EX, A] = {
        val fp = ExprOp3_2F.fixpoint[Free[?[_], A], EX]
        import fp._

        $trunc(expr)
      }
    }

  def handle2_6[T[_[_]]]: FuncHandler[T, Expr2_6] =
    handleOpsCore(trunc2_6[Expr2_6])
  def handle3_0[T[_[_]]]: FuncHandler[T, Expr3_0] =
    handleOps3_0 orElse
    handleOpsCore(trunc2_6[Expr3_0])
  def handle3_2[T[_[_]]]: FuncHandler[T, Expr3_2] =
    handleOps3_2[T].orElse[ExprOp3_0F, Expr3_2](
    handleOps3_0[T]) orElse
    handleOpsCore(trunc3_2[Expr3_2])
}

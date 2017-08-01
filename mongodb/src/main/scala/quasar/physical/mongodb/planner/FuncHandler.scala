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

package quasar.physical.mongodb.planner

import slamdata.Predef._
import quasar.physical.mongodb.{Bson, BsonCodec}
import quasar.physical.mongodb.expression._
import quasar.qscript.{Coalesce => _, MapFuncsDerived => D,  _}, MapFuncsCore._

import scala.Predef.implicitly

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz.{Divide => _, _}, Scalaz._

trait FuncHandler[IN[_]] {

  def handleOpsCore[EX[_]: Functor](trunc: Free[EX, ?] ~> Free[EX, ?])(implicit e26: ExprOpCoreF :<: EX)
      : IN ~> OptionFree[EX, ?]

  def handleOps3_0[EX[_]: Functor](implicit e26: ExprOpCoreF :<: EX, e30: ExprOp3_0F :<: EX)
      : IN ~> OptionFree[EX, ?]

  def handleOps3_2[EX[_]: Functor](implicit e26: ExprOpCoreF :<: EX, e30: ExprOp3_0F :<: EX, e32: ExprOp3_2F :<: EX)
      : IN ~> OptionFree[EX, ?]

  def trunc2_6[EX[_]: Functor](implicit inj: ExprOpCoreF :<: EX): Free[EX, ?] ~> Free[EX, ?] =
    new (Free[EX, ?] ~> Free[EX, ?]) {
      def apply[A](expr: Free[EX, A]): Free[EX, A] = {
        val fp = new ExprOpCoreF.fixpoint[Free[EX, A], EX](Free.roll)
        import fp._

        $subtract(expr, $mod(expr, $literal(Bson.Int32(1))))
      }
    }

  def trunc3_2[EX[_]: Functor](implicit inj: ExprOp3_2F :<: EX): Free[EX, ?] ~> Free[EX, ?] =
    new (Free[EX, ?] ~> Free[EX, ?]) {
      def apply[A](expr: Free[EX, A]): Free[EX, A] = {
        val fp = new ExprOp3_2F.fixpoint[Free[EX, A], EX](Free.roll)
        import fp._

        $trunc(expr)
      }
    }

  def handle2_6
      : IN ~> OptionFree[Expr2_6, ?] =
    λ[IN ~> OptionFree[Expr2_6, ?]]{f =>
      val h = handleOpsCore[Expr2_6](trunc2_6)
      h(f)
    }

  def handle3_0
      : IN ~> OptionFree[Expr3_0, ?] =
    λ[IN ~> OptionFree[Expr3_0, ?]]{f =>
      val h30 = handleOps3_0[Expr3_0]
      val h = handleOpsCore[Expr3_0](trunc2_6)
      h30(f) orElse h(f)
    }

  def handle3_2
     : IN ~> OptionFree[Expr3_2, ?] =
    λ[IN ~> OptionFree[Expr3_2, ?]]{f =>
      val h32 = handleOps3_2[Expr3_2]
      val h30 = handleOps3_0[Expr3_2]
      val h = handleOpsCore[Expr3_2](trunc3_2)
      h32(f) orElse h30(f) orElse h(f)
    }
}

object FuncHandler {

  implicit def mapFuncCore[T[_[_]]: BirecursiveT]: FuncHandler[MapFuncCore[T, ?]] =
    new FuncHandler[MapFuncCore[T, ?]] {

      def handleOpsCore[EX[_]: Functor](trunc: Free[EX, ?] ~> Free[EX, ?])(implicit e26: ExprOpCoreF :<: EX)
          : MapFuncCore[T, ?] ~> OptionFree[EX, ?] =
        new (MapFuncCore[T, ?] ~> OptionFree[EX, ?]) {

          implicit def hole[D](d: D): Free[EX, D] = Free.pure(d)

          def apply[A](mfc: MapFuncCore[T, A]): OptionFree[EX, A] = {

            val fp = new ExprOpCoreF.fixpoint[Free[EX, A], EX](Free.roll)
            import fp._

            def partial(mfc: MapFuncCore[T, A]): OptionFree[EX, A] = mfc.some collect {
              case Undefined()           => $literal(Bson.Undefined)
              case Add(a1, a2)           => $add(a1, a2)
              case Multiply(a1, a2)      => $multiply(a1, a2)
              case Subtract(a1, a2)      => $subtract(a1, a2)
              case Divide(a1, a2)        =>
                // TODO
                // 1) remove workaround for appropriate Mongo version driver
                // once this Mongo issue is fixed: https://jira.mongodb.org/browse/SERVER-29410
                // 2) it would be nice if we would be able to generate simply $divide(a1, a2) for
                // $literal's != 0 (but the type of a2 is generic so we can't check it here)
                $cond($eq(a2, $literal(Bson.Int32(0))),
                  $cond($eq(a1, $literal(Bson.Int32(0))),
                    $literal(Bson.Dec(Double.NaN)),
                    $cond($gt(a1, $literal(Bson.Int32(0))),
                      $literal(Bson.Dec(Double.PositiveInfinity)),
                      $literal(Bson.Dec(Double.NegativeInfinity)))),
                  $divide(a1, a2))
              case Modulo(a1, a2)        => $mod(a1, a2)
              case Negate(a1)            => $multiply($literal(Bson.Int32(-1)), a1)
              case MapFuncsCore.Eq(a1, a2)   => $eq(a1, a2)
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
                  $subtract(a1, $literal(Bson.Date(0))),
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
               $add($literal(Bson.Date(0)), a1)

              case Between(a1, a2, a3)   => $and($lte(a2, a1), $lte(a1, a3))
              // TODO: With type info, we could reduce the number of comparisons necessary.
              case TypeOf(a1) =>
               $cond($lt(a1, $literal(Bson.Null)),                             $literal(Bson.Undefined),
                 $cond($eq(a1, $literal(Bson.Null)),                           $literal(Bson.Text("null")),
                   // TODO: figure out how to distinguish integer
                   $cond($lt(a1, $literal(Bson.Text(""))),                     $literal(Bson.Text("decimal")),
                     // TODO: Once we’re encoding richer types, we need to check for metadata here.
                     $cond($lt(a1, $literal(Bson.Doc())),                      $literal(Bson.Text("array")),
                       $cond($lt(a1, $literal(Bson.Arr())),                    $literal(Bson.Text("map")),
                         $cond($lt(a1, $literal(Bson.ObjectId(Check.minOid))), $literal(Bson.Text("array")),
                           $cond($lt(a1, $literal(Bson.Bool(false))),          $literal(Bson.Text("_bson.objectid")),
                             $cond($lt(a1, $literal(Check.minDate)),           $literal(Bson.Text("boolean")),
                               $cond($lt(a1, $literal(Check.minTimestamp)),    $literal(Bson.Text("_ejson.timestamp")),
                                 // FIXME: This only sorts distinct from Date in 3.0+, so we have to be careful … somehow.
                                 $cond($lt(a1, $literal(Check.minRegex)),      $literal(Bson.Text("_bson.timestamp")),
                                                                               $literal(Bson.Text("_bson.regularexpression"))))))))))))
            }

            partial(mfc) orElse (mfc match {
              case Constant(v1)  => v1.cataM(BsonCodec.fromEJson).toOption.map($literal(_))
              case _             => None
            })
          }
        }

        def handleOps3_0[EX[_]: Functor](implicit e26: ExprOpCoreF :<: EX, e30: ExprOp3_0F :<: EX)
            : MapFuncCore[T, ?] ~> OptionFree[EX, ?] =
          new (MapFuncCore[T, ?] ~> OptionFree[EX, ?]){
            implicit def hole[D](d: D): Free[EX, D] = Free.pure(d)

            def apply[A](mfc: MapFuncCore[T, A]): OptionFree[EX, A] = {
              val fp = new ExprOp3_0F.fixpoint[Free[EX, A], EX](Free.roll)
              import fp._
              import FormatSpecifier._

              mfc.some collect {
               case TimeOfDay(a1) =>
                 $dateToString(Hour :: ":" :: Minute :: ":" :: Second :: "." :: Millisecond :: FormatString.empty, a1)
              }
            }
          }

        def handleOps3_2[EX[_]: Functor]
            (implicit e26: ExprOpCoreF :<: EX, e30: ExprOp3_0F :<: EX, e32: ExprOp3_2F :<: EX)
            : MapFuncCore[T, ?] ~> OptionFree[EX, ?] =
          new (MapFuncCore[T, ?] ~> OptionFree[EX, ?]){
            implicit def hole[D](d: D): Free[EX, D] = Free.pure(d)

            def apply[A](mfc: MapFuncCore[T, A]): OptionFree[EX, A] = {
              val fp = new ExprOp3_2F.fixpoint[Free[EX, A], EX](Free.roll)
              import fp._

              mfc.some collect {
                case Power(a1, a2) =>
                  $pow(a1, a2)
              }
            }
          }
    }

  def mapFuncDerived[T[_[_]]: CorecursiveT]
      : FuncHandler[MapFuncDerived[T, ?]] =
    new FuncHandler[MapFuncDerived[T, ?]] {
      def emptyDerived[T[_[_]], F[_]]: MapFuncDerived[T, ?] ~> OptionFree[F, ?] =
         λ[MapFuncDerived[T, ?] ~> OptionFree[F, ?]] { _ => None }

      def handleOpsCore[EX[_]: Functor](trunc: Free[EX, ?] ~> Free[EX, ?])(implicit e26: ExprOpCoreF :<: EX)
          : MapFuncDerived[T, ?] ~> OptionFree[EX, ?] =
        emptyDerived

      def handleOps3_0[EX[_]: Functor](implicit e26: ExprOpCoreF :<: EX, e30: ExprOp3_0F :<: EX)
          : MapFuncDerived[T, ?] ~> OptionFree[EX, ?] =
        emptyDerived

      def handleOps3_2[EX[_]: Functor]
          (implicit e26: ExprOpCoreF :<: EX, e30: ExprOp3_0F :<: EX, e32: ExprOp3_2F :<: EX)
          : MapFuncDerived[T, ?] ~> OptionFree[EX, ?] =
        new (MapFuncDerived[T, ?] ~> OptionFree[EX, ?]){
          implicit def hole[D](d: D): Free[EX, D] = Free.pure(d)

          def apply[A](fa: MapFuncDerived[T, A]): OptionFree[EX, A] = {
            val fp = new ExprOp3_2F.fixpoint[Free[EX, A], EX](Free.roll)
            import fp._

            fa.some collect {
              case D.Abs(a1)       => $abs(a1)
              case D.Ceil(a1)      => $ceil(a1)
              case D.Floor(a1)     => $floor(a1)
              case D.Trunc(a1)     => $trunc(a1)
            }
          }
        }
    }

  implicit def mapFuncDerivedUnhandled[T[_[_]]: CorecursiveT]
    (implicit core: FuncHandler[MapFuncCore[T, ?]])
      : FuncHandler[MapFuncDerived[T, ?]] =
    new FuncHandler[MapFuncDerived[T, ?]] {
      val derived = mapFuncDerived

      private def handleUnhandled[F[_]]
        (derived: MapFuncDerived[T, ?] ~> OptionFree[F, ?], core: MapFuncCore[T, ?] ~> OptionFree[F, ?])
          : MapFuncDerived[T, ?] ~> OptionFree[F, ?] =
            new (MapFuncDerived[T, ?] ~> OptionFree[F, ?]) {
              def apply[A](f: MapFuncDerived[T, A]): OptionFree[F, A] = {
                val alg: AlgebraM[Option, CoEnv[A, MapFuncCore[T,?], ?], Free[F,A]] =
                  _.run.fold[OptionFree[F, A]](x => Free.point(x).some, core(_).map(_.join))
                derived(f)
                  .orElse(Free.roll(ExpandMapFunc.mapFuncDerived[T, MapFuncCore[T, ?]].expand(f)).cataM(alg))
            }
          }

      def handleOpsCore[EX[_]: Functor](trunc: Free[EX, ?] ~> Free[EX, ?])(implicit e26: ExprOpCoreF :<: EX)
          : MapFuncDerived[T, ?] ~> OptionFree[EX, ?] =
        handleUnhandled(derived.handleOpsCore(trunc), core.handleOpsCore(trunc))

      def handleOps3_0[EX[_]: Functor](implicit e26: ExprOpCoreF :<: EX, e30: ExprOp3_0F :<: EX)
          : MapFuncDerived[T, ?] ~> OptionFree[EX, ?] =
        handleUnhandled(derived.handleOps3_0, core.handleOps3_0)

      def handleOps3_2[EX[_]: Functor]
          (implicit e26: ExprOpCoreF :<: EX, e30: ExprOp3_0F :<: EX, e32: ExprOp3_2F :<: EX)
          : MapFuncDerived[T, ?] ~> OptionFree[EX, ?] =
        handleUnhandled(derived.handleOps3_2, core.handleOps3_2)
    }

  implicit def mapFuncCoproduct[F[_], G[_]]
      (implicit F: FuncHandler[F], G: FuncHandler[G])
      : FuncHandler[Coproduct[F, G, ?]] =
    new FuncHandler[Coproduct[F, G, ?]] {
      def handleOpsCore[EX[_]: Functor](trunc: Free[EX, ?] ~> Free[EX, ?])(implicit e26: ExprOpCoreF :<: EX)
          : Coproduct[F, G, ?] ~> OptionFree[EX, ?] =
        λ[Coproduct[F, G, ?] ~> OptionFree[EX, ?]](_.run.fold(
          F.handleOpsCore(trunc).apply _,
          G.handleOpsCore(trunc).apply _
        ))

      def handleOps3_0[EX[_]: Functor](implicit e26: ExprOpCoreF :<: EX, e30: ExprOp3_0F :<: EX)
          : Coproduct[F, G, ?] ~> OptionFree[EX, ?] =
        λ[Coproduct[F, G, ?] ~> OptionFree[EX, ?]](_.run.fold(
          F.handleOps3_0[EX].apply _,
          G.handleOps3_0[EX].apply _
        ))

      def handleOps3_2[EX[_]: Functor]
          (implicit e26: ExprOpCoreF :<: EX, e30: ExprOp3_0F :<: EX, e32: ExprOp3_2F :<: EX)
          : Coproduct[F, G, ?] ~> OptionFree[EX, ?] =
        λ[Coproduct[F, G, ?] ~> OptionFree[EX, ?]](_.run.fold(
          F.handleOps3_2[EX].apply _,
          G.handleOps3_2[EX].apply _
        ))
    }

  def handle2_6[F[_]: FuncHandler]: F ~> OptionFree[Expr2_6, ?] =
    implicitly[FuncHandler[F]].handle2_6

  def handle3_0[F[_]: FuncHandler]: F ~> OptionFree[Expr3_0, ?] =
    implicitly[FuncHandler[F]].handle3_0

  def handle3_2[F[_]: FuncHandler]: F ~> OptionFree[Expr3_2, ?] =
    implicitly[FuncHandler[F]].handle3_2

}

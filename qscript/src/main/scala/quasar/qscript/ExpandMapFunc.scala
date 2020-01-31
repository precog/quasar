/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.qscript

import slamdata.Predef._

import quasar.ejson.EJson
import quasar.qscript.{MapFuncsDerived => D}, MapFuncsCore._

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz.{Divide => _, _}, Scalaz._
import simulacrum._

@typeclass trait ExpandMapFunc[IN[_]] {
  type OUT[_]

  def expand: IN ~> ((OUT ∘ Free[OUT, ?])#λ)
}

object ExpandMapFunc extends ExpandMapFuncInstances {
  type Aux[IN[_], OUTʹ[_]] = ExpandMapFunc[IN] {
    type OUT[A] = OUTʹ[A]
  }

  def expand[T[_[_]]: CorecursiveT, F[_]: Monad, A]
    (core: AlgebraM[F, MapFuncCore[T, ?], A],
      derived: AlgebraM[(Option ∘ F)#λ, MapFuncDerived[T, ?], A])
      : AlgebraM[F, MapFuncDerived[T, ?], A] = { f =>
    derived(f).getOrElse(
      Free.roll(mapFuncDerived[T, MapFuncCore[T, ?]].expand(f)).cataM[F, A](
        interpretM[F, MapFuncCore[T, ?], A, A](scala.Predef.implicitly[Monad[F]].point[A](_), core)))
  }
}

sealed abstract class ExpandMapFuncInstances extends ExpandMapFuncInstancesʹ {

  implicit def mapFuncDerived[T[_[_]]: CorecursiveT, OUTʹ[_]]
    (implicit MFC: MapFuncCore[T, ?] :<: OUTʹ, OUTʹ: Functor[OUTʹ])
      : ExpandMapFunc.Aux[MapFuncDerived[T, ?], OUTʹ] =
    new ExpandMapFunc[MapFuncDerived[T, ?]] {
      type OUT[A] = OUTʹ[A]
      type OutFree[A] = (OUT ∘ Free[OUT, ?])#λ[A]

      def intR[A](i: Int): Free[OUT, A] =
        Free.roll(MFC(Constant(EJson.int(i))))

      def decR[A](d: BigDecimal): Free[OUT, A] =
        Free.roll(MFC(Constant(EJson.dec(d))))

      def truncR[A](a: Free[OUT, A]): Free[OUT, A] =
        Free.roll[OUT, A](trunc(a))

      def trunc[A](a: Free[OUT, A]): (OUT ∘ Free[OUT, ?])#λ[A] =
        MFC(Subtract(
          a,
          moduloR(a, intR(1))))

      def addR[A](a1: Free[OUT, A], a2: Free[OUT, A]): Free[OUT, A] =
        Free.roll(MFC(Add(a1, a2)))

      def subtractR[A](a1: Free[OUT, A], a2: Free[OUT, A]): Free[OUT, A] =
        Free.roll(MFC(Subtract(a1, a2)))

      def multiplyR[A](a1: Free[OUT, A], a2: Free[OUT, A]): Free[OUT, A] =
        Free.roll(MFC(Multiply(a1, a2)))

      def divideR[A](a1: Free[OUT, A], a2: Free[OUT, A]): Free[OUT, A] =
        Free.roll(MFC(Divide(a1, a2)))

      def powerR[A](a1: Free[OUT, A], a2: Free[OUT, A]): Free[OUT, A] =
        Free.roll(MFC(Power(a1, a2)))

      def floor[A](a: Free[OUT, A]): (OUT ∘ Free[OUT, ?])#λ[A] =
        MFC(Cond(
          eqR(moduloR(a, intR(1)), intR(0)),
          a,
          condR(
            ltR(a, intR(0)),
            subtractR(truncR(a), intR(1)),
            truncR(a))))

      def floorR[A](a: Free[OUT, A]): Free[OUT, A] =
        Free.roll(floor(a))

      def undefinedR[A]: Free[OUT, A] =
        Free.roll(MFC(Undefined()))

      // CREATE FUNCTION ROUND(:a) BEGIN
      //   CASE
      //     WHEN :a % 1 = 0 THEN :a
      //     WHEN abs(:a % 1) = 0.5 THEN
      //       CASE
      //         WHEN CEIL(:a) % 2 = 0 THEN CEIL(:a)
      //         WHEN FLOOR(:a)
      //       END
      //     WHEN (abs(:a % 1) < 0.5 & :a > 0) | (abs(:a % 1) > 0.5 & :a < 0) THEN FLOOR(:a)
      //     ELSE CEIL(:a)
      //   END
      // END
      def round[A](a: Free[OUT, A]): (OUT ∘ Free[OUT, ?])#λ[A] =
        MFC(Cond(
          eqR(moduloR(a, intR(1)), intR(0)),
          a,
          condR(
            eqR(absR(moduloR(a, intR(1))), decR(0.5)),
            condR(
              eqR(moduloR(ceilR(a), intR(2)), intR(0)),
              ceilR(a),
              floorR(a)),
            condR(
              orR(
                andR(
                  ltR(absR(moduloR(a, intR(1))), decR(0.5)),
                  gtR(a, intR(0))),
                andR(
                  gtR(absR(moduloR(a, intR(1))), decR(0.5)),
                  ltR(a, intR(0)))),
              floorR(a),
              ceilR(a)))))

      def roundR[A](a: Free[OUT, A]): Free[OUT, A] =
        Free.roll(round(a))

      def ceil[A](a: Free[OUT, A]): (OUT ∘ Free[OUT, ?])#λ[A] =
        MFC(Cond(
          eqR(moduloR(a, intR(1)), intR(0)),
          a,
          condR(
            ltR(a, intR(0)),
            truncR(a),
            addR(truncR(a), intR(1)))))

      def ceilR[A](a: Free[OUT, A]): Free[OUT, A] =
        Free.roll(ceil(a))

      def moduloR[A](a1: Free[OUT, A], a2: Free[OUT, A]): Free[OUT, A] =
        Free.roll(MFC(Modulo(a1, a2)))

      def eqR[A](a1: Free[OUT, A], a2: Free[OUT, A]): Free[OUT, A] =
        Free.roll(MFC(MapFuncsCore.Eq(a1, a2)))

      def ltR[A](a1: Free[OUT, A], a2: Free[OUT, A]): Free[OUT, A] =
        Free.roll(MFC(Lt(a1, a2)))

      def gtR[A](a1: Free[OUT, A], a2: Free[OUT, A]): Free[OUT, A] =
        Free.roll(MFC(Gt(a1, a2)))

      def andR[A](a1: Free[OUT, A], a2: Free[OUT, A]): Free[OUT, A] =
        Free.roll(MFC(MapFuncsCore.And(a1, a2)))

      def orR[A](a1: Free[OUT, A], a2: Free[OUT, A]): Free[OUT, A] =
        Free.roll(MFC(MapFuncsCore.Or(a1, a2)))

      def negateR[A](a: Free[OUT, A]): Free[OUT, A] =
        Free.roll(MFC(Negate(a)))

      def absR[A](a: Free[OUT, A]): Free[OUT, A] =
        Free.roll(abs(a))

      def abs[A](a: Free[OUT, A]): (OUT ∘ Free[OUT, ?])#λ[A] =
        MFC(Cond(
          ltR(a, intR(0)),
          negateR(a),
          a))

      def condR[A](a1: Free[OUT, A], a2: Free[OUT, A], a3: Free[OUT, A]): Free[OUT, A] =
        Free.roll(MFC(Cond(a1, a2, a3)))

      val expand: MapFuncDerived[T, ?] ~> ((OUT ∘ Free[OUT, ?])#λ) =
        λ[MapFuncDerived[T, ?] ~> (OUT ∘ Free[OUT, ?])#λ] {
          case D.Abs(a) => abs(a.point[Free[OUT, ?]])
          case D.Ceil(a) => ceil(a.point[Free[OUT, ?]])
          case D.Floor(a) => floor(a.point[Free[OUT, ?]])
          case D.Trunc(a) => trunc(a.point[Free[OUT, ?]])
          case D.Round(a) => round(a.point[Free[OUT, ?]])

          // CREATE FUNCTION FLOOR_SCALE(:a, :b) BEGIN FLOOR(:a * 10.0 ^ :b) / 10.0 ^ :b END;
          case D.FloorScale(a, b) =>
            MFC(Divide(
              floorR(
                multiplyR(
                  a.point[Free[OUT, ?]],
                  powerR(intR(10), b.point[Free[OUT, ?]]))),
              powerR(intR(10), b.point[Free[OUT, ?]])))

          // CREATE FUNCTION CEIL_SCALE(:val, :n) BEGIN CEIL(:val * 10.0 ^ :n) / 10.0 ^ :n END;
          case D.CeilScale(a, b) =>
            MFC(Divide(
              ceilR(
                multiplyR(
                  a.point[Free[OUT, ?]],
                  powerR(intR(10), b.point[Free[OUT, ?]]))),
              powerR(intR(10), b.point[Free[OUT, ?]])))

          // CREATE FUNCTION ROUND_SCALE(:val, :n) BEGIN ROUND(:val * 10.0 ^ :n) / 10.0 ^ :n END;
          case D.RoundScale(a, b) =>
            MFC(Divide(
              roundR(
                multiplyR(
                  a.point[Free[OUT, ?]],
                  powerR(intR(10), b.point[Free[OUT, ?]]))),
              powerR(intR(10), b.point[Free[OUT, ?]])))

          case D.Typecheck(a, typ) =>
            MFC(Guard(a.point[Free[OUT, ?]], typ, a.point[Free[OUT, ?]], undefinedR))
        }
    }

  implicit def coproduct[T[_[_]]: CorecursiveT, F[_], G[_], OUTʹ[_]]
    (implicit F: ExpandMapFunc.Aux[F, OUTʹ], G: ExpandMapFunc.Aux[G, OUTʹ])
      : ExpandMapFunc.Aux[Coproduct[F, G, ?], OUTʹ] =
    new ExpandMapFunc[Coproduct[F, G, ?]] {
      type OUT[A] = OUTʹ[A]

      val expand: Coproduct[F, G, ?] ~> ((OUT ∘ Free[OUT, ?])#λ) =
        λ[Coproduct[F, G, ?] ~> (OUT ∘ Free[OUT, ?])#λ] {
          case Coproduct(-\/(f)) => F.expand(f)
          case Coproduct(\/-(g)) => G.expand(g)
        }
    }
}

sealed abstract class ExpandMapFuncInstancesʹ {
  implicit def default[T[_[_]]: CorecursiveT, IN[_]: Functor, OUTʹ[_]]
    (implicit IN: IN :<: OUTʹ, OUTʹ: Functor[OUTʹ])
      : ExpandMapFunc.Aux[IN, OUTʹ] =
    new ExpandMapFunc[IN] {
      type OUT[A] = OUTʹ[A]

      val expand: IN ~> ((OUT ∘ Free[OUT, ?])#λ) =
        λ[IN ~> (OUT ∘ Free[OUT, ?])#λ] {
          f => IN(f.map(_.point[Free[OUT, ?]]))
        }
    }
}

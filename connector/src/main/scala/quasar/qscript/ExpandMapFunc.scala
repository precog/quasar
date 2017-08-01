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

package quasar.qscript

import slamdata.Predef._

import quasar.ejson
import quasar.qscript.{MapFuncsDerived => D}, MapFuncsCore._

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._
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
      Free.roll(mapFuncDerived[T, MapFuncCore[T, ?]].expand(f)).cataM(
        interpretM(scala.Predef.implicitly[Monad[F]].point[A](_), core)))
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
        Free.roll(MFC(Constant(ejson.EJson.fromExt(ejson.int(i)))))

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

      def moduloR[A](a1: Free[OUT, A], a2: Free[OUT, A]): Free[OUT, A] =
        Free.roll(MFC(Modulo(a1, a2)))

      def eqR[A](a1: Free[OUT, A], a2: Free[OUT, A]): Free[OUT, A] =
        Free.roll(MFC(MapFuncsCore.Eq(a1, a2)))

      def ltR[A](a1: Free[OUT, A], a2: Free[OUT, A]): Free[OUT, A] =
        Free.roll(MFC(Lt(a1, a2)))

      def negateR[A](a: Free[OUT, A]): Free[OUT, A] =
        Free.roll(MFC(Negate(a)))

      def condR[A](a1: Free[OUT, A], a2: Free[OUT, A], a3: Free[OUT, A]): Free[OUT, A] =
        Free.roll(MFC(Cond(a1, a2, a3)))

      val expand: MapFuncDerived[T, ?] ~> ((OUT ∘ Free[OUT, ?])#λ) =
        λ[MapFuncDerived[T, ?] ~> (OUT ∘ Free[OUT, ?])#λ] {
          case D.Abs(a) =>
            MFC(Cond(
              ltR(a.point[Free[OUT, ?]], intR(0)),
              negateR(a.point[Free[OUT, ?]]),
              a.point[Free[OUT, ?]]))
          case D.Ceil(a) =>
            MFC(Cond(
              eqR(moduloR(a.point[Free[OUT, ?]], intR(1)), intR(0)),
              a.point[Free[OUT, ?]],
              condR(
                ltR(a.point[Free[OUT, ?]], intR(0)),
                truncR(a.point[Free[OUT, ?]]),
                addR(truncR(a.point[Free[OUT, ?]]), intR(1)))))
          case D.Floor(a) =>
            MFC(Cond(
              eqR(moduloR(a.point[Free[OUT, ?]], intR(1)), intR(0)),
              a.point[Free[OUT, ?]],
              condR(
                ltR(a.point[Free[OUT, ?]], intR(0)),
                subtractR(truncR(a.point[Free[OUT, ?]]), intR(1)),
                truncR(a.point[Free[OUT, ?]]))))
          case D.Trunc(a) => trunc(a.point[Free[OUT, ?]])
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

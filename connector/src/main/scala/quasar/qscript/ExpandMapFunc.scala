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

import quasar.ejson
import quasar.qscript.{MapFuncsDerived => D}, MapFuncsCore._

import matryoshka._
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
}

sealed abstract class ExpandMapFuncInstances extends ExpandMapFuncInstancesʹ {

  implicit def mapFuncDerived[T[_[_]]: CorecursiveT, OUTʹ[_]]
    (implicit MFC: MapFuncCore[T, ?] :<: OUTʹ, OUTʹ: Functor[OUTʹ])
      : ExpandMapFunc.Aux[MapFuncDerived[T, ?], OUTʹ] =
    new ExpandMapFunc[MapFuncDerived[T, ?]] {
      type OUT[A] = OUTʹ[A]

      val expand: MapFuncDerived[T, ?] ~> ((OUT ∘ Free[OUT, ?])#λ) =
        λ[MapFuncDerived[T, ?] ~> (OUT ∘ Free[OUT, ?])#λ] {
          case D.Abs(a) =>
            MFC(Cond(
              Free.roll(MFC(Lt(a.point[Free[OUT,?]], Free.roll(MFC(Constant(ejson.EJson.fromExt(ejson.int(0)))))))),
              Free.roll(MFC(Negate(a.point[Free[OUT, ?]]))),
              a.point[Free[OUT, ?]]
            ))
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

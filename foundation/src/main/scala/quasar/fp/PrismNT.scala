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

package quasar.fp

import slamdata.Predef._
import matryoshka._
import matryoshka.patterns.CoEnv
import monocle.Prism
import scalaz._

/** Just like Prism, but operates over Functors.
  */
final case class PrismNT[F[_], G[_]](get: F ~> (Option ∘ G)#λ, reverseGet: G ~> F) {
  def apply[A](ga: G[A]): F[A] = reverseGet(ga)

  def unapply[A](fa: F[A]): Option[G[A]] = get(fa)

  def andThen[H[_]](other: PrismNT[G, H]): PrismNT[F, H] =
    PrismNT(
      λ[F ~> (Option ∘ H)#λ](f => get(f).flatMap(g => other.get(g))),
      reverseGet compose other.reverseGet)

  def compose[H[_]](other: PrismNT[H, F]): PrismNT[H, G] =
    other andThen this

  def asPrism[A]: Prism[F[A], G[A]] =
    Prism(get.apply[A])(reverseGet.apply[A])
}

object PrismNT {
  def id[F[_]]: PrismNT[F, F] =
    PrismNT(λ[F ~> (Option ∘ F)#λ](Some(_)), reflNT[F])

  def inject[F[_], G[_]](implicit I: F :<: G): PrismNT[G, F] =
    PrismNT(λ[G ~> (Option ∘ F)#λ](I.prj(_)), λ[F ~> G](I.inj(_)))

  def injectCopK[F[_], G[a] <: ACopK[a]](implicit I: F :<<: G): PrismNT[G, F] =
    PrismNT(I.prj, I.inj)

  def coEnv[F[_], A]: PrismNT[CoEnv[A, F, ?], F] =
    PrismNT(
      λ[CoEnv[A, F, ?] ~> λ[α => Option[F[α]]]](_.run.toOption),
      λ[F ~> CoEnv[A, F, ?]](fb => CoEnv(\/-(fb))))
}

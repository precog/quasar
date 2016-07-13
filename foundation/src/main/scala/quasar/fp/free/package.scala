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

package quasar.fp

import quasar.Predef._

import scalaz._

package object free {
  sealed abstract class :+:[F[_], G[_]] {
    type λ[A] = Coproduct[F, G, A]
  }

  implicit class EnrichNT[F[_], H[_]](f: F ~> H) {
    def :+:[G[_]](g: G ~> H): (G :+: F)#λ ~> H = new ((G :+: F)#λ ~> H) {
      def apply[A](fa: (G :+: F)#λ[A]) = fa.run.fold(g, f)
    }
  }

  def flatMapSNT[S[_], T[_]](f: S ~> Free[T, ?]): Free[S, ?] ~> Free[T, ?] =
    new (Free[S, ?] ~> Free[T, ?]) {
      def apply[A](fa: Free[S, A]) = fa.flatMapSuspension(f)
    }

  def foldMapNT[F[_], G[_]: Monad](f: F ~> G) = new (Free[F, ?] ~> G) {
    def apply[A](fa: Free[F, A]): G[A] = fa.foldMap(f)
  }

  /** `Inject#inj` as a natural transformation. */
  def injectNT[F[_], G[_]](implicit I: F :<: G): F ~> G =
    new (F ~> G) {
      def apply[A](fa: F[A]) = I inj fa
    }

  /** Convenience transformation to inject into a coproduct and lift into
    * `Free`.
    */
  def injectFT[F[_], S[_]](implicit S: F :<: S): F ~> Free[S, ?] =
    liftFT[S] compose injectNT[F, S]

  /** Given `F[_]` and `G[_]` such that both `:<: H`, lifts a natural
    * transformation `F ~> G` to `H ~> H`.
    */
  object injectedNT {
    def apply[H[_]] = new Aux[H]

    final class Aux[H[_]] {
      def apply[F[_], G[_]](f: F ~> G)(implicit F: F :<: H, G: G :<: H): H ~> H =
        new (H ~> H) {
          def apply[A](ga: H[A]) = F.prj(ga).fold(ga)(fa => G.inj(f(fa)))
        }
    }
  }

  /** `Free#liftF` as a natural transformation */
  def liftFT[S[_]]: S ~> Free[S, ?] =
    new (S ~> Free[S, ?]) {
      def apply[A](s: S[A]) = Free.liftF(s)
    }

  def mapSNT[S[_], T[_]](f: S ~> T): Free[S, ?] ~> Free[T, ?] =
    new (Free[S, ?] ~> Free[T, ?]) {
      def apply[A](fa: Free[S, A]) = fa.mapSuspension(f)
    }

  def restrict[M[_], S[_], T[_]](f: T ~> M)(implicit S: S :<: T) =
    f compose injectNT[S, T]

  /** Given `F[_]` and `S[_]` such that `F :<: S`, returns a natural
    * transformation, `S ~> G`, where `f` is used to transform an `F[_]` and `g`
    * used otherwise.
    */
  def transformIn[F[_], S[_], G[_]](f: F ~> G, g: S ~> G)(implicit S: F :<: S): S ~> G =
    new (S ~> G) {
      def apply[A](sa: S[A]) = S.prj(sa).fold(g(sa))(f)
    }

  def transform2In[F[_], H[_], S[_], G[_]](f1: F ~> G, f2: H ~> G, g: S ~> G)(implicit S: F :<: S, S1: H :<: S): S ~> G =
    transformIn(f2, transformIn(f1, g))
}

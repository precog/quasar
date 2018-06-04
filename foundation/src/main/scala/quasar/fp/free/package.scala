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

import scalaz._
import slamdata.Predef.Option

package object free {
  sealed abstract class :+:[F[_], G[_]] {
    type λ[A] = Coproduct[F, G, A]
  }
  implicit class EnrichNT[F[_], H[_]](f: F ~> H) {
    def :+:[G[_]](g: G ~> H) = λ[(G :+: F)#λ ~> H](_.run.fold(g, f))
  }

  def mapSNT[S[_], T[_]](f: S ~> T)              = λ[Free[S, ?] ~> Free[T, ?]](_ mapSuspension f)
  def flatMapSNT[S[_], T[_]](f: S ~> Free[T, ?]) = λ[Free[S, ?] ~> Free[T, ?]](_ flatMapSuspension f)
  def foldMapNT[F[_], G[_]: Monad](f: F ~> G)    = λ[Free[F, ?] ~> G](_ foldMap f)

  /** `Inject#inj` as a natural transformation. */
  def injectNT[F[_], G[_]](implicit I: F :<: G) = λ[F ~> G](I inj _)

  /** `Inject#prj` as a natural transformation. */
  def projectNT[F[_], G[_]](implicit I: F :<: G) = λ[G ~> λ[a => Option[F[a]]]](I prj _)

  /** Convenience transformation to inject into a coproduct and lift into Free. */
  def injectFT[F[_], S[_]](implicit S: F :<: S): F ~> Free[S, ?] = liftFT[S] compose injectNT[F, S]

  /** `Free#liftF` as a natural transformation */
  def liftFT[S[_]] = λ[S ~> Free[S, ?]](Free liftF _)

  /** Given `F[_]` and `S[_]` such that `F :<: S`, returns a natural
    * transformation, `S ~> G`, where `f` is used to transform an `F[_]` and `g`
    * used otherwise.
    */
  def transformIn[F[_], S[_], G[_]](f: F ~> G, g: S ~> G)(implicit S: F :<: S) =
    λ[S ~> G](sa => (S prj sa).fold(g(sa))(f))
}

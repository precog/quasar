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

package quasar.contrib

import quasar.fp._
import quasar.fp.ski._

import _root_.matryoshka._, Recursive.ops._, TraverseT.ops._
import _root_.matryoshka.patterns._
import scalaz._, Scalaz._

package object matryoshka extends CoEnvInstances {

  implicit def equalTEqual[T[_[_]], F[_]: Functor]
    (implicit T: EqualT[T], F: Delay[Equal, F])
      : Equal[T[F]] =
    T.equalT[F]

  implicit def showTShow[T[_[_]], F[_]: Functor]
    (implicit T: ShowT[T], F: Delay[Show, F]):
      Show[T[F]] =
    T.showT[F]

  def elgotM[M[_]: Monad, F[_]: Traverse, A, B](a: A)(φ: F[B] => M[B], ψ: A => M[B \/ F[A]]):
      M[B] = {
    def h(a: A): M[B] = ψ(a) >>= (_.traverse(_.traverse(h) >>= φ).map(_.merge))
    h(a)
  }
  /** Algebra transformation that allows a standard algebra to be used on a
    * CoEnv structure (given a function that converts the leaves to the result
    * type).
    */
  def interpret[F[_], A, B](f: A => B, φ: Algebra[F, B]):
      Algebra[CoEnv[A, F, ?], B] =
    interpretM[Id, F, A, B](f, φ)

  def interpretM[M[_], F[_], A, B](f: A => M[B], φ: AlgebraM[M, F, B]):
      AlgebraM[M, CoEnv[A, F, ?], B] =
    ginterpretM[Id, M, F, A, B](f, φ)

  def ginterpretM[W[_], M[_], F[_], A, B](f: A => M[B], φ: GAlgebraM[W, M, F, B]):
      GAlgebraM[W, M, CoEnv[A, F, ?], B] =
    _.run.fold(f, φ)

  /** A specialization of `interpret` where the leaves are of the result type.
    */
  def recover[F[_], A](φ: Algebra[F, A]): Algebra[CoEnv[A, F, ?], A] =
    interpret(ι, φ)

  // TODO: This should definitely be in Matryoshka.
  // apomorphism - short circuit by returning left
  def substitute[T[_[_]], F[_]](original: T[F], replacement: T[F])(implicit T: Equal[T[F]]):
      T[F] => T[F] \/ T[F] =
   tf => if (tf ≟ original) replacement.left else tf.right

  // TODO: This should definitely be in Matryoshka.
  def transApoT[T[_[_]]: FunctorT, F[_]: Functor](t: T[F])(f: T[F] => T[F] \/ T[F]):
      T[F] =
    f(t).fold(ι, FunctorT[T].map(_)(_.map(transApoT(_)(f))))

  def freeCata[F[_]: Functor, E, A](free: Free[F, E])(φ: Algebra[CoEnv[E, F, ?], A]): A =
    free.hylo(φ, CoEnv.freeIso[E, F].reverseGet)

  def freeCataM[M[_]: Monad, F[_]: Traverse, E, A](free: Free[F, E])(φ: AlgebraM[M, CoEnv[E, F, ?], A]): M[A] =
    free.hyloM(φ, CoEnv.freeIso[E, F].reverseGet(_).point[M])

  def freeGcataM[W[_]: Comonad: Traverse, M[_]: Monad, F[_]: Traverse, E, A](
    free: Free[F, E])(
    k: DistributiveLaw[CoEnv[E, F, ?], W],
    φ: GAlgebraM[W, M, CoEnv[E, F, ?], A]):
      M[A] =
    free.ghyloM[W, Id, M, CoEnv[E, F, ?], A](k, distAna, φ, CoEnv.freeIso[E, F].reverseGet(_).point[M])

  def distTraverse[F[_]: Traverse, G[_]: Applicative] =
    new DistributiveLaw[F, G] {
      def apply[A](fga: F[G[A]]) = fga.sequence
    }

  // TODO[matryoshka]: Should be an HMap instance
  def coEnvHmap[F[_], G[_], A](f: F ~> G) =
    λ[CoEnv[A, F, ?] ~> CoEnv[A, G, ?]](fa => CoEnv(fa.run.map(f(_))))

  // TODO[matryoshka]: Should be an HTraverse instance
  def coEnvHtraverse[G[_]: Applicative, F[_], H[_], A](f: F ~> (G ∘ H)#λ) =
    λ[CoEnv[A, F, ?] ~> (G ∘ CoEnv[A, H, ?])#λ](_.run.traverse(f(_)).map(CoEnv(_)))

  def envtHmap[F[_], G[_], E, A](f: F ~> G) =
    λ[EnvT[E, F, ?] ~> EnvT[E, G, ?]](env => EnvT((env.ask, f(env.lower))))

  def envtHtraverse[G[_]: Applicative, F[_], H[_], A](f: F ~> (G ∘ H)#λ) =
    λ[EnvT[A, F, ?] ~> (G ∘ EnvT[A, H, ?])#λ](_.run.traverse(f(_)).map(EnvT(_)))

  implicit final class CoEnvOps[T[_[_]], F[_], E](val self: T[CoEnv[E, F, ?]]) extends scala.AnyVal {
    final def fromCoEnv(implicit fa: Functor[F], tr: Recursive[T]): Free[F, E] =
      self.cata(CoEnv.freeIso[E, F].get)
  }

  /** Applies a transformation over `Free`, treating it like `T[CoEnv]`.
    */
  def freeTransCata[T[_[_]]: Recursive: Corecursive, F[_]: Functor, G[_]: Functor, A, B](
    free: Free[F, A])(
    f: CoEnv[A, F, T[CoEnv[B, G, ?]]] => CoEnv[B, G, T[CoEnv[B, G, ?]]]):
      Free[G, B] =
    free.toCoEnv[T].transCata[CoEnv[B, G, ?]](f).fromCoEnv

  def freeTransCataM[T[_[_]]: Recursive: Corecursive, M[_]: Monad, F[_]: Traverse, G[_]: Functor, A, B](
    free: Free[F, A])(
    f: CoEnv[A, F, T[CoEnv[B, G, ?]]] => M[CoEnv[B, G, T[CoEnv[B, G, ?]]]]):
      M[Free[G, B]] =
    free.toCoEnv[T].transCataM[M, CoEnv[B, G, ?]](f) ∘ (_.fromCoEnv)

  def transFutu[T[_[_]]: FunctorT: Corecursive, F[_]: Functor, G[_]: Traverse]
    (t: T[F])
    (f: GCoalgebraicTransform[T, Free[G, ?], F, G]):
      T[G] =
    FunctorT[T].map(t)(f(_).copoint.map(freeCata(_)(interpret[G, T[F], T[G]](transFutu(_)(f), _.embed))))

  def freeTransFutu[T[_[_]]: Recursive: Corecursive, F[_]: Functor, G[_]: Traverse, A, B]
    (free: Free[F, A])
    (f: CoEnv[A, F, T[CoEnv[A, F, ?]]] => CoEnv[B, G, Free[CoEnv[B, G, ?], T[CoEnv[A, F, ?]]]])
      : Free[G, B] =
    transFutu(free.toCoEnv[T])(f).fromCoEnv

  implicit def envtEqual[E: Equal, F[_]](implicit F: Delay[Equal, F]):
      Delay[Equal, EnvT[E, F, ?]] =
    new Delay[Equal, EnvT[E, F, ?]] {
      def apply[A](eq: Equal[A]) =
        Equal.equal {
          case (env1, env2) =>
            env1.ask ≟ env2.ask && F(eq).equal(env1.lower, env2.lower)
        }
    }

  implicit def envtShow[E: Show, F[_]](implicit F: Delay[Show, F]):
      Delay[Show, EnvT[E, F, ?]] =
    new Delay[Show, EnvT[E, F, ?]] {
      def apply[A](sh: Show[A]) =
        Show.show {
          envt => Cord("EnvT(") ++ envt.ask.show ++ Cord(", ") ++ F(sh).show(envt.lower) ++ Cord(")")
        }
    }

  implicit def envtTraverse[F[_]: Traverse, X]: Traverse[EnvT[X, F, ?]] = new Traverse[EnvT[X, F, ?]] {
    def traverseImpl[G[_]: Applicative, A, B](envT: EnvT[X, F, A])(f: A => G[B]): G[EnvT[X, F, B]] =
      envT.run match {
        case (x, fa) => fa.traverse(f).map(fb => EnvT((x, fb)))
      }
  }

  def envtLowerNT[F[_], E]                  = λ[EnvT[E, F, ?] ~> F](_.lower)
}

trait LowPriorityCoEnvImplicits {
  implicit def coenvTraverse[F[_]: Traverse, E]: Traverse[CoEnv[E, F, ?]] =
    CoEnv.bitraverse[F, E].rightTraverse
}

trait CoEnvInstances extends LowPriorityCoEnvImplicits {
  implicit def coenvFunctor[F[_]: Functor, E]: Functor[CoEnv[E, F, ?]] =
    CoEnv.bifunctor[F].rightFunctor
}

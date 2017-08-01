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

package quasar.fp

import slamdata.Predef._
import matryoshka._
import matryoshka.implicits._
import scalaz._, Scalaz._

package object binder {
  /** Annotate (the original nodes of) a tree, by applying a function to the
    * "bound" nodes. The function is also applied to the bindings themselves
    * to determine their annotation.
    */
  def boundAttribute[T, F[_]: Functor, A]
    (t: T)
    (f: T => A)
    (implicit TR: Recursive.Aux[T, F], TC: Corecursive.Aux[T, F], B: Binder[F])
      : Cofree[F, A] = {
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def loop(t: F[T], b: B.G[(T, Cofree[F, A])]): (T, Cofree[F, A]) = {
      val newB = B.bindings(t, b)(loop(_, b))
      B.subst(t, newB).fold {
        val m: F[(T, Cofree[F, A])] = t.map(x => loop(x.project, newB))
        val t1 = m.map(_._1).embed
        (t1, Cofree(f(t1), m.map(_._2)))
      } { case (x, _) => (x, t.embed.cata(attrK(f(x)))) }
    }
    loop(t.project, B.initial)._2
  }

  def boundCata[T, F[_]: Functor, A]
    (t: T)
    (f: F[A] => A)
    (implicit T: Recursive.Aux[T, F], B: Binder[F])
      : A = {
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))  
    def loop(t: F[T], b: B.G[A]): A = {
      val newB = B.bindings(t, b)(loop(_, b))
      B.subst(t, newB).getOrElse(f(t.map(x => loop(x.project, newB))))
    }
    loop(t.project, B.initial)
  }

  def boundParaM[T, M[_]: Monad, F[_]: Traverse, A]
    (t: T)
    (f: F[(T, A)] => M[A])
    (implicit T: Recursive.Aux[T, F], B: Binder[F])
      : M[A] = {
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def loop(t: F[T], b: B.G[A]): M[A] = {
      Applicative[M].sequence(B.bindings[T, M[A]](t, B.G.map(b)(_.point[M]))(s => loop(s, b)))(B.G).flatMap { newB =>
        B.subst(t, newB).cata[M[A]](
          _.point[M],
          t.traverse(x => loop(x.project, newB).map((x, _))).flatMap(f))
      }
    }
    loop(t.project, B.initial)
  }

  def boundParaS[T, F[_]: Traverse, S, A]
    (t: T)
    (f: F[(T, A)] => State[S, A])
    (implicit T: Recursive.Aux[T, F], B: Binder[F])
      : State[S, A] =
    boundParaM[T, State[S, ?], F, A](t)(f)

  def boundPara[T, F[_]: Functor, A]
    (t: T)
    (f: F[(T, A)] => A)
    (implicit T: Recursive.Aux[T, F], B: Binder[F])
      : A = {
    @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
    def loop(t: F[T], b: B.G[A]): A = {
      val newB = B.bindings(t, b)(loop(_, b))
      B.subst(t, newB).getOrElse(f(t.map(x => (x, loop(x.project, newB)))))
    }
    loop(t.project, B.initial)
  }
}

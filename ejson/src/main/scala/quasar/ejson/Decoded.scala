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

package quasar.ejson

import slamdata.Predef.{Option, String}
import quasar.{RenderTree, RenderedTree}
import quasar.fp.ski.κ
import quasar.contrib.iota.copkTraverse

import matryoshka.Recursive
import scalaz.{\/, -\/, \/-, Applicative, Cord, Equal, Monad, Plus, Show, Traverse}
import scalaz.std.string._
import scalaz.std.tuple._
import scalaz.syntax.functor._
import scalaz.syntax.show._

/** The result of attempting to decode a `A` from EJson. */
final case class Decoded[A](toDisjunction: (RenderedTree, String) \/ A) {
  def fold[B](f: (RenderedTree, String) => B, s: A => B): B =
    toDisjunction.fold(t => f(t._1, t._2), s)

  def map[B](f: A => B): Decoded[B] =
    Decoded(toDisjunction map f)

  def flatMap[B](f: A => Decoded[B]): Decoded[B] =
    Decoded(toDisjunction flatMap (f andThen (_.toDisjunction)))

  def orElse(other: => Decoded[A]): Decoded[A] =
    Decoded(toDisjunction orElse other.toDisjunction)

  def setMessage(msg: String): Decoded[A] =
    withMessage(κ(msg))

  def toOption: Option[A] =
    toDisjunction.toOption

  def withMessage(f: String => String): Decoded[A] =
    Decoded(toDisjunction.leftMap(_.map(f)))
}

object Decoded extends DecodedInstances {
  def attempt[J, A](j: => J, r: String \/ A)(implicit J: Recursive.Aux[J, EJson]): Decoded[A] =
    r.fold(Decoded.failureFor[A](j, _), Decoded.success)

  def failure[A](input: RenderedTree, msg: String): Decoded[A] =
    Decoded(-\/((input, msg)))

  object failureFor {
    def apply[A] = new PartiallyApplied[A]
    final class PartiallyApplied[A] {
      def apply[J](input: J, msg: String)(implicit J: Recursive.Aux[J, EJson]): Decoded[A] =
        failure[A](RenderTree.recursive.render(input), msg)
    }
  }

  def success[A](a: A): Decoded[A] =
    Decoded(\/-(a))
}

sealed abstract class DecodedInstances {
  implicit val covariant: Monad[Decoded] with Traverse[Decoded] =
    new Monad[Decoded] with Traverse[Decoded] {
      override def map[A, B](fa: Decoded[A])(f: A => B) =
        fa map f

      def bind[A, B](fa: Decoded[A])(f: A => Decoded[B]) =
        fa flatMap f

      def point[A](a: => A) =
        Decoded.success(a)

      def traverseImpl[F[_]: Applicative, A, B](fa: Decoded[A])(f: A => F[B]): F[Decoded[B]] =
        fa.toDisjunction.traverse(f).map(Decoded(_))
    }

  implicit def equal[A: Equal]: Equal[Decoded[A]] =
    Equal.equalBy(_.toDisjunction)

  implicit val plus: Plus[Decoded] =
    new Plus[Decoded] {
      def plus[A](a: Decoded[A], b: => Decoded[A]): Decoded[A] =
        a orElse b
    }

  implicit def show[A: Show]: Show[Decoded[A]] =
    Show.show(d => Cord("Decoded") ++ d.fold(
      (t, m) => (t, m).show,
      a => Cord("(") ++ a.show ++ Cord(")")))
}

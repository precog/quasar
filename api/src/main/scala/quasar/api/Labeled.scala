/*
 * Copyright 2020 Precog Data
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

package quasar.api

import cats.{Applicative, Eq, Eval, Order, Show, Traverse}
import cats.implicits._

import scala.{Predef, StringContext}, Predef._

import java.lang.String

/**
 * Otherwise known as "the Free Label".
 */
final case class Labeled[+A](label: String, value: A)

private[api] trait LowPriorityImplicits {
  implicit def eq[A: Eq]: Eq[Labeled[A]] =
    Eq.by(l => (l.label, l.value))
}

object Labeled extends LowPriorityImplicits {

  implicit def label[A]: Label[Labeled[A]] =
    Label.label(_.label)

  implicit def show[A: Show]: Show[Labeled[A]] =
    Show.show(l => s"Labeled(${l.label}, ${l.value.show})")

  implicit def order[A: Order]: Order[Labeled[A]] =
    Order.by(l => (l.label, l.value))

  implicit val traverse: Traverse[Labeled] = new Traverse[Labeled] {

    def foldLeft[A, B](fa: Labeled[A], b: B)(f: (B, A) => B): B =
      f(b, fa.value)

    def foldRight[A, B](fa: Labeled[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] =
      f(fa.value, lb)

    override def map[A, B](fa: Labeled[A])(f: A => B): Labeled[B] =
      Labeled(fa.label, f(fa.value))

    def traverse[G[_]: Applicative, A, B](fa: Labeled[A])(f: A => G[B]): G[Labeled[B]] =
      f(fa.value).map(Labeled(fa.label, _))
  }
}

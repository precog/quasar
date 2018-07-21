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

package quasar

import cats.Monoid

trait CatsSpecs2Instances {
  implicit def specs2ToCatsMonoid[A](implicit specsMonoid: org.specs2.fp.Monoid[A]): Monoid[A] =
    specs2ToCatsMonoid0(specsMonoid)

  ////

  private def specs2ToCatsMonoid0[A](specsMonoid: org.specs2.fp.Monoid[A]): Monoid[A] =
    new Monoid[A] {
      def empty: A = specsMonoid.zero
      def combine(f1: A, f2: A): A = specsMonoid.append(f1, f2)
    }
}

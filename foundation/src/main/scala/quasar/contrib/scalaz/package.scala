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

package quasar.contrib

import slamdata.Predef._

import _root_.scalaz._, \&/._, Scalaz._

package object scalaz {
  def -\&/[A, B](a: A): These[A, B] = This(a)
  def \&/-[A, B](b: B): These[A, B] = That(b)

  object HasThis {
    def unapply[A](these: A \&/ _): Option[A] = these match {
      case Both(a, _) => a.some
      case This(a)    => a.some
      case That(_)    => none
    }
  }

  object HasThat {
    def unapply[B](these: _ \&/ B): Option[B] = these match {
      case Both(_, b) => b.some
      case This(_)    => none
      case That(b)    => b.some
    }
  }

  implicit def toMonadTell_Ops[F[_], W, A](fa: F[A])(implicit F: MonadTell_[F, W]): MonadTell_Ops[F, W, A] =
    new MonadTell_Ops[F, W, A](fa)

  implicit def toMonadError_Ops[F[_], E, A](fa: F[A])(implicit F: MonadError_[F, E]): MonadError_Ops[F, E, A] =
    new MonadError_Ops[F, E, A](fa)
}

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

package quasar

import scalaz._

package object niflheim {

  /**
   * Dear god don't use this!  It's a shim to make old things work.  This is NOT
   * a lawful monad!
   */
  @deprecated
  private[niflheim] implicit def validationMonad[E]: Monad[Validation[E, ?]] =
    new Monad[Validation[E, ?]] {
      def point[A](a: => A) = Success(a)

      def bind[A, B](fa: Validation[E, A])(f: A => Validation[E, B]): Validation[E, B] =
        fa.fold(Failure(_), f)
    }
}

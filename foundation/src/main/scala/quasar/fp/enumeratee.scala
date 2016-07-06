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
import scalaz.iteratee._

object enumeratee {
  def take[I, F[_]: Monad](n: Int): EnumerateeT[I, I, F] =
    new EnumerateeT[I, I, F] {
      import Iteratee._
      def apply[A] = {
        def loop(i: Int) =
          step(i) andThen cont[I, F, StepT[I, F, A]]

        def step(i: Int): (Input[I] => IterateeT[I, F, A]) => Input[I] => IterateeT[I, F, StepT[I, F, A]] = {
          k => in =>
            in(
              el = e =>
                if (i <= 0) cont(step(i)(k))
                else k(in) >>== doneOr(loop(i - 1))
              , empty = cont(step(i)(k))
              , eof = done(scont(k), in)
            )
        }

        doneOr(loop(n))
      }
    }
}

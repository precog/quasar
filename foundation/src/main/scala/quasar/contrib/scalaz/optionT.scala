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

package quasar.contrib.scalaz

import slamdata.Predef._

import scalaz._, Scalaz._

trait OptionTInstances {
  implicit def optionTCatchable[F[_]: Catchable : Functor]: Catchable[OptionT[F, ?]] =
    new Catchable[OptionT[F, ?]] {
      def attempt[A](fa: OptionT[F, A]) =
        OptionT[F, Throwable \/ A](
          Catchable[F].attempt(fa.run) map {
            case -\/(t)  => Some(\/.left(t))
            case \/-(oa) => oa map (\/.right)
          })

      def fail[A](t: Throwable) =
        OptionT[F, A](Catchable[F].fail(t))
    }
}

object optionT extends OptionTInstances

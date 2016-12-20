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

import scalaz._, Scalaz._


trait StateTInstances {

  implicit def stateTMonadError_[M[_] : Monad, S, E](implicit
    merr: MonadError_[M, E]
  ): MonadError_[StateT[M, S, ?], E] =
    new MonadError_[StateT[M, S, ?], E] {
      def raiseError[A](e: E): StateT[M, S, A] =
        StateT((s: S) => merr.raiseError[A](e).map(a => (s, a)))

      def handleError[A](fa: StateT[M, S, A])(f: E => StateT[M, S, A]): StateT[M, S, A] =
        StateT{(s: S) =>
          (merr.handleError[A](fa.run(s).map(_._2))((e: E) => f(e).run(s).map(_._2))).map((s, _))
        }
    }
}

object stateT extends StateTInstances

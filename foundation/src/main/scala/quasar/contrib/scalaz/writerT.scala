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

import slamdata.Predef.Throwable

import scalaz._, Scalaz._

trait WriterTInstances {
  implicit def writerTCatchable[F[_]: Catchable : Functor, W: Monoid]: Catchable[WriterT[F, W, ?]] =
    new Catchable[WriterT[F, W, ?]] {
      def attempt[A](fa: WriterT[F, W, A]) =
        WriterT[F, W, Throwable \/ A](
          Catchable[F].attempt(fa.run) map {
            case -\/(t)      => (mzero[W], t.left)
            case \/-((w, a)) => (w, a.right)
          })

      def fail[A](t: Throwable) =
        WriterT(Catchable[F].fail(t).strengthL(mzero[W]))
    }
}

object writerT extends WriterTInstances

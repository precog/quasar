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

package quasar.effect

import slamdata.Predef._
import quasar.fp.TaskRef

import monocle.Lens
import scalaz.{Lens => _, _}
import scalaz.concurrent.Task
import scalaz.syntax.applicative._

/** Provides the ability to request the next element of a monotonically
  * increasing numeric sequence.
  *
  * That is,
  *
  *   for {
  *     a <- next
  *     b <- next
  *   } yield a < b
  *
  * must always be true.
  */
sealed abstract class MonotonicSeq[A]

object MonotonicSeq {
  case object Next extends MonotonicSeq[Long]

  final class Ops[S[_]](implicit S: MonotonicSeq :<: S)
    extends LiftedOps[MonotonicSeq, S] {

    def next: FreeS[Long] =
      lift(Next)
  }

  object Ops {
    implicit def apply[S[_]](implicit S: MonotonicSeq :<: S): Ops[S] =
      new Ops[S]
  }

  def fromZero: Task[MonotonicSeq ~> Task] =
    TaskRef(0L).map(fromTaskRef)

  /** Returns an interpreter of `MonotonicSeq` into `Task`, given a
    * `TaskRef[Long]`.
    */
  def fromTaskRef(ref: TaskRef[Long]): MonotonicSeq ~> Task =
    new (MonotonicSeq ~> Task) {
      val toST = toState[State[Long, ?]](Lens.id[Long])
      def apply[A](fa: MonotonicSeq[A]) =
        ref.modifyS(toST(fa).run)
    }

  /** Returns an interpreter of `MonotonicSeq` into `F[S, ?]`,
    * given a `Lens[S, Long]` and `MonadState[F, S]`.
    *
    * NB: Uses partial application of `F[_, _]` for better type inference, usage:
    *   `toState[F](lens)`
    */
  object toState {
    def apply[F[_]]: Aux[F] =
      new Aux[F]

    final class Aux[F[_]] {
      def apply[S](l: Lens[S, Long])(implicit F: MonadState[F, S]): MonotonicSeq ~> F =
        λ[MonotonicSeq ~> F]{ case Next => F.gets(l.get) <* F.modify(l.modify(_ + 1)) }
    }
  }
}

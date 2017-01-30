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

package quasar.effect

import quasar.Predef._

import simulacrum.typeclass
import scalaz._, Scalaz._

/** Represents the ability to request the next element of a monotonically
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
@typeclass
trait MonoSeq[F[_]] {
  def next: F[Long]
}

object MonoSeq extends MonoSeqInstances {
  def forTrans[F[_]: Monad, T[_[_], _]: MonadTrans](implicit S: MonoSeq[F]): MonoSeq[T[F, ?]] =
    new MonoSeq[T[F, ?]] { def next = S.next.liftM[T] }

  def forInjectable[F[_], S[_]](implicit F: MonoSeq[F], I: F :<: S): MonoSeq[Free[S, ?]] =
    new MonoSeq[Free[S, ?]] {
      def next = Free.liftF(I(F.next))
    }
}

sealed abstract class MonoSeqInstances {
  implicit val monotonicSeq: MonoSeq[MonotonicSeq] =
    new MonoSeq[MonotonicSeq] {
      val next = MonotonicSeq.Next
    }

  implicit def injectableMonotonicSeq[S[_]](implicit I: MonotonicSeq :<: S): MonoSeq[Free[S, ?]] =
    MonoSeq.forInjectable[MonotonicSeq, S]

  implicit def eitherTMonoSeq[F[_]: Monad: MonoSeq, E]: MonoSeq[EitherT[F, E, ?]] =
    MonoSeq.forTrans[F, EitherT[?[_], E, ?]]

  implicit def readerTMonoSeq[F[_]: Monad: MonoSeq, R]: MonoSeq[ReaderT[F, R, ?]] =
    MonoSeq.forTrans[F, ReaderT[?[_], R, ?]]

  implicit def stateTMonoSeq[F[_]: Monad: MonoSeq, S]: MonoSeq[StateT[F, S, ?]] =
    MonoSeq.forTrans[F, StateT[?[_], S, ?]]

  implicit def writerTMonoSeq[F[_]: Monad: MonoSeq, W: Monoid]: MonoSeq[WriterT[F, W, ?]] =
    MonoSeq.forTrans[F, WriterT[?[_], W, ?]]
}

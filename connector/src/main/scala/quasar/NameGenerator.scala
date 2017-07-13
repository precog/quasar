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

import slamdata.Predef._
import quasar.fp.ski.κ
import quasar.effect.MonotonicSeq

import simulacrum.typeclass
import scalaz._
import scalaz.concurrent.Task
import scalaz.syntax.functor._

/** A source of strings unique within `F[_]`, an implementation must have the
  * property that, if Applicative[F], then (freshName |@| freshName)(_ != _).
  */
@typeclass trait NameGenerator[F[_]] {
  /** Returns a fresh name, guaranteed to be unique among all the other names
    * generated from `F`.
    */
  def freshName: F[String]

  /** Returns a fresh name, prefixed with the given string. */
  def prefixedName(prefix: String)(implicit F: Functor[F]): F[String] =
    freshName map (prefix + _)
}

object NameGenerator extends NameGeneratorInstances {
  /** A short, randomized string to use as "salt" in salted name generators. */
  val salt: Task[String] =
    Task.delay(scala.util.Random.nextInt().toHexString)
}

sealed abstract class NameGeneratorInstances extends NameGeneratorInstances0 {
  implicit def sequenceNameGenerator[F[_]](implicit F: MonadState[F, Long]): NameGenerator[F] =
    new NameGenerator[F] {
      def freshName = F.bind(F.get)(n => F.put(n + 1) as n.toString)
    }

  implicit def monotonicSeqNameGenerator[S[_]](implicit S: MonotonicSeq :<: S): NameGenerator[Free[S, ?]] =
    new NameGenerator[Free[S, ?]] {
      def freshName = MonotonicSeq.Ops[S].next map (_.toString)
    }
}

sealed abstract class NameGeneratorInstances0 {
  implicit def eitherTNameGenerator[F[_]: NameGenerator : Functor, A]: NameGenerator[EitherT[F, A, ?]] =
    new NameGenerator[EitherT[F, A, ?]] {
      def freshName = EitherT.right(NameGenerator[F].freshName)
    }

  implicit def readerTNameGenerator[F[_]: NameGenerator, A]: NameGenerator[ReaderT[F, A, ?]] =
    new NameGenerator[ReaderT[F, A, ?]] {
      def freshName = ReaderT(κ(NameGenerator[F].freshName))
    }

  implicit def stateTNameGenerator[F[_]: NameGenerator : Monad, S]: NameGenerator[StateT[F, S, ?]] =
    new NameGenerator[StateT[F, S, ?]] {
      def freshName = StateT(s => NameGenerator[F].freshName strengthL s)
    }

  implicit def writerTNameGenerator[F[_]: NameGenerator : Functor, W: Monoid]: NameGenerator[WriterT[F, W, ?]] =
    new NameGenerator[WriterT[F, W, ?]] {
      def freshName = WriterT.put(NameGenerator[F].freshName)(Monoid[W].zero)
    }
}

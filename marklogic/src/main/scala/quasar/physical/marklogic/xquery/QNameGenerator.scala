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

package quasar.physical.marklogic.xquery

import quasar.Predef._
import quasar.fp.ski.κ
import quasar.effect.MonoSeq
import quasar.physical.marklogic.xml.{NCName, QName}

import eu.timepit.refined.api.Refined
import simulacrum.typeclass
import scalaz._
import scalaz.syntax.functor._

/** A source of QNames unique within `F[_]`, an implementation must have the
  * property that, if Applicative[F], then (freshQName |@| freshQName)(_ != _).
  */
@typeclass trait QNameGenerator[F[_]] {
  /** Returns a fresh QName, guaranteed to be unique among all the other QNames
    * generated from `F`.
    */
  def freshQName: F[QName]
}

object QNameGenerator extends QNameGeneratorInstances

sealed abstract class QNameGeneratorInstances extends QNameGeneratorInstances0 {
  implicit def sequenceQNameGenerator[F[_]](implicit F: MonadState[F, Long]): QNameGenerator[F] =
    new QNameGenerator[F] {
      def freshQName = F.bind(F.get)(n => F.put(n + 1) as numericQName(n))
    }

  implicit def monotonicSeqQNameGenerator[F[_]: Functor](implicit F: MonoSeq[F]): QNameGenerator[F] =
    new QNameGenerator[F] {
      def freshQName = MonoSeq[F].next map (numericQName)
    }

  private def numericQName(n: Long): QName = {
    val name = if (n < 0) s"n_${scala.math.abs(n)}" else s"n$n"
    QName.local(NCName(Refined.unsafeApply(name)))
  }
}

sealed abstract class QNameGeneratorInstances0 {
  implicit def eitherTQNameGenerator[F[_]: QNameGenerator : Functor, A]: QNameGenerator[EitherT[F, A, ?]] =
    new QNameGenerator[EitherT[F, A, ?]] {
      def freshQName = EitherT.right(QNameGenerator[F].freshQName)
    }

  implicit def readerTQNameGenerator[F[_]: QNameGenerator, A]: QNameGenerator[ReaderT[F, A, ?]] =
    new QNameGenerator[ReaderT[F, A, ?]] {
      def freshQName = ReaderT(κ(QNameGenerator[F].freshQName))
    }

  implicit def stateTQNameGenerator[F[_]: QNameGenerator : Monad, S]: QNameGenerator[StateT[F, S, ?]] =
    new QNameGenerator[StateT[F, S, ?]] {
      def freshQName = StateT(s => QNameGenerator[F].freshQName strengthL s)
    }

  implicit def writerTQNameGenerator[F[_]: QNameGenerator : Functor, W: Monoid]: QNameGenerator[WriterT[F, W, ?]] =
    new QNameGenerator[WriterT[F, W, ?]] {
      def freshQName = WriterT.put(QNameGenerator[F].freshQName)(Monoid[W].zero)
    }
}

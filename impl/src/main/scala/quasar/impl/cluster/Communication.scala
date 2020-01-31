/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.impl.cluster

import slamdata.Predef._

import cats.Contravariant

import fs2.Stream

import scodec.Codec

abstract class Communication[F[_], Id, T] {
  def unicast[P: Codec](tag: T, payload: P, target: Id): F[Unit]
  def multicast[P: Codec](tag: T, payload: P, targets: Set[Id]): F[Unit]
  def subscribe[P: Codec](tag: T, limit: Int): F[Stream[F, (Id, P)]]
}

object Communication {
  implicit def contravariantCommunication[F[_], Id]: Contravariant[Communication[F, Id, ?]] = new Contravariant[Communication[F, Id, ?]] {
    def contramap[A, B](fa: Communication[F, Id, A])(f: B => A): Communication[F, Id, B] = new Communication[F, Id, B] {
      def unicast[P: Codec](tag: B, payload: P, target: Id): F[Unit] =
        fa.unicast(f(tag), payload, target)
      def multicast[P: Codec](tag: B, payload: P, targets: Set[Id]): F[Unit] =
        fa.multicast(f(tag), payload, targets)
      def subscribe[P: Codec](tag: B, limit: Int): F[Stream[F, (Id, P)]] =
        fa.subscribe(f(tag), limit)
    }
  }
}

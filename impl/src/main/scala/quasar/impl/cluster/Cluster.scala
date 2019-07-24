/*
 * Copyright 2014–2018 SlamData Inc.
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

abstract class Cluster[F[_], T] {
  type Id
  def gossip[P: Codec](tag: T, p: P): F[Unit]
  def subscribe[P: Codec](tag: T, limit: Int): F[Stream[F, (Id, P)]]
  def unicast[P: Codec](tag: T, p: P, id: Id): F[Unit]
  def broadcast[P: Codec](tag: T, p: P): F[Unit]
}

object Cluster {
  implicit def clusterContravariant[F[_]]: Contravariant[Cluster[F, ?]] = new Contravariant[Cluster[F, ?]] {
    def contramap[A, B](fa: Cluster[F, A])(f: B => A): Cluster[F, B] = new Cluster[F, B] {
      type Id = fa.Id
      def gossip[P: Codec](tag: B, p: P): F[Unit] = fa.gossip(f(tag), p)
      def subscribe[P: Codec](tag: B, limit: Int): F[Stream[F, (Id, P)]] = fa.subscribe(f(tag), limit)
      def unicast[P: Codec](tag: B, p: P, id: Id): F[Unit] = fa.unicast(f(tag), p, id)
      def broadcast[P: Codec](tag: B, p: P): F[Unit] = fa.broadcast(f(tag), p)
    }
  }
}

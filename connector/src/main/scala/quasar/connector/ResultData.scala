/*
 * Copyright 2020 Precog Data
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

package quasar.connector

import slamdata.Predef._
import quasar.api.push.ExternalOffsetKey

import cats._
import fs2.{Stream, Chunk}

sealed trait ResultData[F[_], A] extends Product with Serializable { self =>
  type Elem = A
  def delimited: Stream[F, Either[ExternalOffsetKey, Chunk[A]]]
  def data: Stream[F, A]
  def mapK[G[_]](f: F ~> G): ResultData[G, A]
}

object ResultData {
  final case class Delimited[F[_], A](
      delimited: Stream[F, Either[ExternalOffsetKey, Chunk[A]]])
      extends ResultData[F, A] {
    def data: Stream[F, A] = delimited.flatMap(_.fold(x => Stream.empty, Stream.chunk))
    def mapK[G[_]](f: F ~> G): ResultData[G, A] = Delimited(delimited.translate[F, G](f))
  }
  final case class Continuous[F[_], A](data: Stream[F, A]) extends ResultData[F, A] {
    def delimited = data.chunks.map(Right(_))
    def mapK[G[_]](f: F ~> G): ResultData[G, A] = Continuous(data.translate[F, G](f))
  }
  implicit def functorResultData[F[_]]: Functor[ResultData[F, *]] = new Functor[ResultData[F, *]] {
    def map[A, B](fa: ResultData[F,A])(f: A => B): ResultData[F,B] = fa match {
      case Continuous(data) => Continuous(data.map(f))
      case Delimited(delimited) => Delimited(delimited.map(_.map(_.map(f))))
    }
  }
  def empty[F[_], A]: ResultData[F, A] = Continuous(Stream.empty)
}

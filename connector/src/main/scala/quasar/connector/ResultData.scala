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
import quasar.api.push
import quasar.common.data.RValue

import cats._
import cats.implicits._
import fs2.{Stream, Chunk}

sealed trait ResultData[F[_], A] extends Product with Serializable { self =>
  type Elem = A
  def delimited: Stream[F, ResultData.Part[A]]
  def data: Stream[F, A]
  def mapK[G[_]](f: F ~> G): ResultData[G, A]
}

object ResultData {

  sealed trait Part[+A] extends Product with Serializable {
    def fold[B](
        f1: push.ExternalOffsetKey => B,
        f2: Option[RValue] => B,
        f3: Chunk[A] => B)
        : B =
      this match {
        case Part.ExternalOffsetKey(k) => f1(k)
        case Part.ContextualData(d) => f2(d)
        case Part.Output(c) => f3(c)
      }
  }

  sealed trait DataPart[+A] extends Part[A] {
    def foldData[B](
        f1: Option[RValue] => B,
        f2: Chunk[A] => B)
        : B =
      this match {
        case Part.ContextualData(d) => f1(d)
        case Part.Output(c) => f2(c)
      }
  }

  object Part {
    final case class ExternalOffsetKey(offsetKey: push.ExternalOffsetKey) extends Part[Nothing]

    val emptyOffsetKey: ExternalOffsetKey = ExternalOffsetKey(push.ExternalOffsetKey.empty)

    def offsetKeyFromBytes(bytes: Array[Byte]): ExternalOffsetKey =
      ExternalOffsetKey(push.ExternalOffsetKey(bytes))

    /**
      * Contextual data pertinent to the output that follows, until another context data annotation is encountered
      * e.g. Stream(C1, O1, O2, C2, O3, C4, C5, O4)
      *
      * C1 pertains to O1 and O2
      * C2 pertains to O3
      * C4 pertains to nothing
      * C5 pertains to O4
      *
      * @param fields
      */
    final case class ContextualData(value: Option[RValue]) extends DataPart[Nothing]

    final case class Output[A](chunk: Chunk[A]) extends DataPart[A]

    implicit def functorPart: Functor[Part] = new Functor[Part] {
      def map[A, B](fa: Part[A])(f: A => B): Part[B] = fa match {
        case Output(c) => Output(c.map(f))
        case e @ ExternalOffsetKey(_) => e
        case c @ ContextualData(_) => c
      }
    }
  }

  final case class Delimited[F[_], A](
      delimited: Stream[F, Part[A]])
      extends ResultData[F, A] {
    def data: Stream[F, A] = delimited.flatMap(_.fold(_ => Stream.empty, _ => Stream.empty, Stream.chunk))
    def mapK[G[_]](f: F ~> G): ResultData[G, A] = Delimited(delimited.translate[F, G](f))
  }

  final case class Continuous[F[_], A](data: Stream[F, A]) extends ResultData[F, A] {
    def delimited = data.chunks.map(Part.Output(_))
    def mapK[G[_]](f: F ~> G): ResultData[G, A] = Continuous(data.translate[F, G](f))
  }

  implicit def functorResultData[F[_]]: Functor[ResultData[F, *]] = new Functor[ResultData[F, *]] {
    def map[A, B](fa: ResultData[F,A])(f: A => B): ResultData[F,B] = fa match {
      case Continuous(data) => Continuous(data.map(f))
      case Delimited(delimited) => Delimited(delimited.map(_.map(f)))
    }
  }

  def empty[F[_], A]: ResultData[F, A] = Continuous(Stream.empty)
}

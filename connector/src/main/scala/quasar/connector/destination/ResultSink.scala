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

package quasar.connector.destination

import slamdata.Predef._

import quasar.api.Column
import quasar.api.push.{OffsetKey, PushColumns}
import quasar.api.resource.ResourcePath
import quasar.connector._
import quasar.connector.render.RenderConfig

import cats.data.NonEmptyList

import fs2.Pipe

import skolems.∀

sealed trait ResultSink[F[_], T] extends Product with Serializable

object ResultSink {
  final case class CreateSink[F[_], T, A](
      consume: (ResourcePath, NonEmptyList[Column[T]]) => (RenderConfig[A], Pipe[F, A, Unit]))
      extends ResultSink[F, T]

  final case class UpsertSink[F[_], T, A](
      consume: UpsertSink.Args[T] => (RenderConfig[A], ∀[λ[α => Pipe[F, DataEvent[A, OffsetKey.Actual[α]], OffsetKey.Actual[α]]]]))
      extends ResultSink[F, T]

  object UpsertSink {
    final case class Args[T](
        path: ResourcePath,
        idColumn: Column[T],
        otherColumns: List[Column[T]],
        writeMode: WriteMode) {

      def columns: NonEmptyList[Column[T]] =
        NonEmptyList(idColumn, otherColumns)
    }
  }

  final case class AppendSink[F[_], T] (
      consume: AppendSink.Args[T] => AppendSink.Result[F])
      extends ResultSink[F, T]

  object AppendSink {
    final case class Args[T](
        path: ResourcePath,
        pushColumns: PushColumns[Column[T]],
        writeMode: WriteMode) {
      def columns: NonEmptyList[Column[T]] =
        pushColumns.toNel
    }
    trait Result[F[_]] {
      type A
      val renderConfig: RenderConfig[A]
      val pipe: ∀[λ[α => Pipe[F, AppendEvent[A, OffsetKey.Actual[α]], OffsetKey.Actual[α]]]]
    }
  }

  def create[F[_], T, A](
      consume: (ResourcePath, NonEmptyList[Column[T]]) => (RenderConfig[A], Pipe[F, A, Unit]))
      : ResultSink[F, T] =
    CreateSink(consume)

  def upsert[F[_], T, A](
      consume: UpsertSink.Args[T] => (RenderConfig[A], ∀[λ[α => Pipe[F, DataEvent[A, OffsetKey.Actual[α]], OffsetKey.Actual[α]]]]))
      : ResultSink[F, T] =
    UpsertSink(consume)

  def append[F[_], T, X](
    consume: AppendSink.Args[T] => (RenderConfig[X], ∀[λ[α => Pipe[F, AppendEvent[X, OffsetKey.Actual[α]], OffsetKey.Actual[α]]]])) : ResultSink[F, T] =
    AppendSink { (args: AppendSink.Args[T]) => consume(args) match {
      case (renderConfig0, pipe0) => new AppendSink.Result[F] {
        type A = X
        val renderConfig = renderConfig0
        val pipe = pipe0
      }
    }}
}

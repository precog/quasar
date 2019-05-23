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

package quasar.impl.datasource.local

import slamdata.Predef.{Stream => _, _}

import quasar.api.resource.ResourcePath
import quasar.concurrent.BlockingContext
import quasar.connector.{Destination, ResultSink, ResultType, TableColumn}

import cats.effect.{ContextShift, Effect}
import fs2.{io, Stream}
import scalaz.NonEmptyList
import scalaz.syntax.monad._
import scalaz.syntax.tag._
import shims._

import java.nio.file.{Path => JPath}

final class LocalDestination[F[_]: Effect: ContextShift] private (
  root: JPath,
  blockingContext: BlockingContext) extends Destination[F] {
  val destinationType = LocalDestinationType

  def sinks: NonEmptyList[ResultSink[F]] =
    NonEmptyList(LocalCsvSink(root, blockingContext))
}

object LocalDestination {
  def apply[F[_]: Effect: ContextShift](
      root: JPath,
      blockingContext: BlockingContext): LocalDestination[F] =
    new LocalDestination[F](root, blockingContext)
}

final class LocalCsvSink[F[_]: Effect: ContextShift] private (
  root: JPath,
  blockingContext: BlockingContext) extends ResultSink[F] {

  val resultType = ResultType.Csv()

  def apply(dst: ResourcePath, result: (List[TableColumn], Stream[F, Byte])): F[Unit] =
    toNio[F](root, dst) >>= { writePath =>
      val (_, bytes) = result
      val sink: Stream[F, Byte] => Stream[F, Unit] =
        io.file.writeAll[F](writePath, blockingContext.unwrap)

      sink(bytes).compile.drain
    }
}

object LocalCsvSink {
  def apply[F[_]: Effect: ContextShift](
    root: JPath,
    blockingContext: BlockingContext): LocalCsvSink[F] = new LocalCsvSink(root, blockingContext)
}

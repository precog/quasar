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

package quasar.mimir

import quasar.Data
import quasar.blueeyes.json.JValue
import quasar.yggdrasil.table.{ColumnarTableModule, Slice}

import cats.effect.IO

import fs2.async
import fs2.async.mutable.Queue

import scalaz.StreamT
import scalaz.syntax.monad._

import shims._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Either, Left, Right}

import java.util.concurrent.atomic.AtomicBoolean

trait TablePagerModule extends ColumnarTableModule {

  final class TablePager private (
      slices: StreamT[IO, Slice],
      queue: Queue[IO, Either[Throwable, Vector[Data]]]) {

    private val running = new AtomicBoolean(true)

    {
      val driver = slices foreachRec { slice =>
        for {
          flag <- IO(running.get())

          _ <- if (flag && !slice.isEmpty) {
            val json = slice.toJsonElements.map(JValue.toData)

            if (json.isEmpty)
              IO.pure(())
            else
              queue.enqueue1(Right(json))
          } else {
            // we can't terminate early, because there are no finalizers in StreamT
            IO.pure(())
          }
        } yield ()
      }

      // enqueue the empty vector so ReadFile.scan knows when to stop scanning
      val ta = driver >> queue.enqueue1(Right(Vector.empty))

      ta unsafeRunAsync {
        case Left(t) => queue.enqueue1(Left(t)).unsafeRunAsync(_ => ())
        case Right(_) => ()
      }
    }

    def more: IO[Vector[Data]] =
      queue.dequeue1.flatMap(IO.fromEither(_))

    def close: IO[Unit] =
      IO(running.set(false))
  }

  object TablePager {
    def apply(table: Table, lookahead: Int = 1): IO[TablePager] = {
      for {
        q <- async.boundedQueue[IO, Either[Throwable, Vector[Data]]](lookahead)
        back <- IO(new TablePager(table.slices, q))
      } yield back
    }
  }
}

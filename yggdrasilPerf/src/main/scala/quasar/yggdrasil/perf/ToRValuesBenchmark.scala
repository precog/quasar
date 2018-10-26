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

package quasar
package yggdrasil
package perf

import quasar.contrib.cats.effect.liftio._
import quasar.precog.common._
import quasar.yggdrasil.table.TestColumnarTableModule

// Must not be in default package
import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext.Implicits.global

import org.openjdk.jmh.annotations.{Benchmark, BenchmarkMode, Mode, OutputTimeUnit, Param, Scope, State}
import org.openjdk.jmh.infra.Blackhole

import cats.effect.IO
import fs2.Stream

import scalaz.WriterT
import scalaz.std.list._
import shims._

object ToRValuesBenchmark {

  implicit val cs = IO.contextShift(global)

  val P = new TestColumnarTableModule {}

  def createAndConsumeTable(data: Stream[IO, RValue], bh: Blackhole): IO[Unit] = {
    val table: WriterT[IO, List[IO[Unit]], P.Table] =
      P.Table.fromQDataStream[WriterT[IO, List[IO[Unit]], ?], RValue](data)
    table.run.flatMap {
      case (_, t) =>
        t.slices.foreachRec(slice => IO(slice.toRValues.foreach(bh.consume)))
    }
  }

}

@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Array(Mode.Throughput))
@State(Scope.Benchmark)
class ToRValuesBenchmarkScalar {

  def scalars = Stream.emits(List.fill[List[RValue]](10)(
    List(CLong(1L), CNum(BigDecimal(1L)), CDouble(1.0), CString("str"))
  ).flatten).covary[IO]

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def scalarsToRValues(bh: Blackhole): Unit = {
    ToRValuesBenchmark.createAndConsumeTable(scalars, bh).unsafeRunSync
  }

}

@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Array(Mode.Throughput))
@State(Scope.Benchmark)
class ToRValuesBenchmarkVector {

  @Param(Array("5", "20", "80", "320", "1280"))
  var vectorLength: Int = _

  def arrays(chunks: Int, chunkSize: Int): Stream[IO, RValue] =
    Stream.constant(
      List.fill(chunkSize)(
        RArray(List.fill(vectorLength)(CLong(1L)))))
    .take(chunks).flatMap(Stream.emits).covary[IO]

  // Each chunk should look like:
  // { k1: v1, k2: v2, k3: v3 }, { k1: v1, k2: v2, k3: v3 }
  def objects(chunks: Int, chunkSize: Int): Stream[IO, RValue] =
    Stream.constant(
      List.fill(chunkSize)(
        RObject(List.tabulate(vectorLength)(i => ("k" + i) -> CLong(1L)).toMap)))
    .take(chunks).flatMap(Stream.emits).covary[IO]

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def arraysToRValues(bh: Blackhole): Unit = {
    ToRValuesBenchmark.createAndConsumeTable(arrays(1, 10), bh).unsafeRunSync
  }

  @Benchmark
  @BenchmarkMode(Array(Mode.AverageTime))
  def objectsToRValues(bh: Blackhole): Unit = {
    ToRValuesBenchmark.createAndConsumeTable(objects(1, 10), bh).unsafeRunSync
  }

}

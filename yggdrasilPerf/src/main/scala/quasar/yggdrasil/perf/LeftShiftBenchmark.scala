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

import quasar.common.CPath
import quasar.contrib.cats.effect.liftio._
import quasar.fp.ski.κ
import quasar.precog.common.{CLong, RArray, RObject, RValue}
import quasar.yggdrasil.table.TestColumnarTableModule

import cats.effect.IO

import java.util.concurrent.TimeUnit
import scala.concurrent.ExecutionContext.Implicits.global

import org.openjdk.jmh.annotations.{Benchmark, BenchmarkMode, Mode, OutputTimeUnit, Param, Scope, Setup, State}
import org.openjdk.jmh.infra.Blackhole

import scalaz._, Scalaz._
import shims._

object LeftShiftBenchmark {

  implicit val cs = IO.contextShift(global)

  val P = new TestColumnarTableModule {}

  def arrays(arraySize: Int): Int => Option[RValue] =
    κ(RArray(List.tabulate(arraySize)(i => CLong(i + 42))).some)

  def objects(nrKeysPerObject: Int): Int => Option[RValue] =
    κ(RObject(List.tabulate(nrKeysPerObject)(i => ("k" + i) -> CLong(i + 42)).toMap).some)

  def distinctFields(nrKeysPerObject: Int): Int => Option[RValue] = { r =>
    RObject(List.tabulate(nrKeysPerObject)(i => ("k" + r + i) -> CLong(i + r + 42)).toMap).some
  }

  def scrolling(nrKeysPerObject: Int, totalNrKeys: Int): Int => Option[RValue] = { r =>
    assert(totalNrKeys >= nrKeysPerObject)
    RObject(List.tabulate(nrKeysPerObject)(i => ("k" + ((r + i) % totalNrKeys)) -> CLong(i + r + 42)).toMap).some
  }

  val scalarUndefined: Int => Option[RValue] = { i =>
    (i % 2) match {
      case 0 => none
      case _ => CLong(i + 42).some
    }
  }

  def heterogeneous(nrKeysPerObject: Int, arraySize: Int): Int => Option[RValue] = { i =>
    (i % 4) match {
      case 0 => none
      case 1 => CLong(i + 42).some
      case 2 => RArray(List.tabulate(arraySize)(i => CLong(i + 42))).some
      case _ => RObject(List.tabulate(nrKeysPerObject)(i => ("k" + i) -> CLong(i + 42)).toMap).some
    }
  }

  def leftShiftRow(key: Int, value: Option[RValue]): RValue =
    value match {
      case None => RArray(CLong(key))
      case Some(v) => RArray(CLong(key), v)
    }

  def leftShiftTestData(chunks: Int, chunkSize: Int, value: Int => Option[RValue]): Stream[List[RValue]] =
    Stream.tabulate(chunks)(c =>
      List.tabulate(chunkSize){ s =>
        val i = (c * chunkSize) + s
        leftShiftRow(i, value(i))
      })

  def mkTable(module: TestColumnarTableModule)(data: Stream[List[RValue]]): IO[module.Table] =
    module.Table.fromQDataStream[WriterT[IO, List[IO[Unit]], ?], RValue](
      fs2.Stream.fromIterator[IO, fs2.Stream[IO, RValue]](
        data.iterator.map(fs2.Stream.emits(_).covary[IO])
      ).join).run.map(_._2)

  abstract class BenchmarkState(v: Int => Option[RValue]) {
    @Param(Array("5"))
    var chunks: Int = _

    @Param(Array("10000"))
    var chunkSize: Int = _

    @Param(Array("true"))
    var emitOnUndef: Boolean = _

    @Param(value = Array("json", "materialize"))
    var consumption: String = _

    var table: P.Table = _

    @Setup
    def setup(): Unit = {
      table = mkTable(P)(leftShiftTestData(chunks, chunkSize, v)).flatMap(_.force).unsafeRunSync
    }
  }

  @State(Scope.Benchmark)
  class BenchmarkState_arrays extends BenchmarkState(arrays(arraySize = 10))

  @State(Scope.Benchmark)
  class BenchmarkState_objects extends BenchmarkState(objects(nrKeysPerObject = 10))

  @State(Scope.Benchmark)
  class BenchmarkState_scrolling extends BenchmarkState(scrolling(nrKeysPerObject = 10, totalNrKeys = 14))

  @State(Scope.Benchmark)
  class BenchmarkState_distinctFields extends BenchmarkState(distinctFields(nrKeysPerObject = 10))

  @State(Scope.Benchmark)
  class BenchmarkState_scalarUndefined extends BenchmarkState(scalarUndefined)

  @State(Scope.Benchmark)
  class BenchmarkState_heterogeneous extends BenchmarkState(heterogeneous(nrKeysPerObject = 10, arraySize = 10))
}

@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Array(Mode.AverageTime))
@State(Scope.Benchmark)
class LeftShiftBenchmark {
  import LeftShiftBenchmark._

  def leftShift(table: P.Table, emitOnUndef: Boolean): P.Table =
    table.leftShift(CPath.Identity \ 1, emitOnUndef)

  def doTestForce(state: BenchmarkState, bh: Blackhole): Unit = {
    val tableAfter: P.Table = leftShift(state.table, state.emitOnUndef)
    SliceTools.consumeTable(P)(state.consumption, tableAfter, bh).unsafeRunSync
  }

  @Benchmark
  def arrays(state: BenchmarkState_arrays, bh: Blackhole): Unit =
    doTestForce(state, bh)

  @Benchmark
  def objects(state: BenchmarkState_objects, bh: Blackhole): Unit =
    doTestForce(state, bh)

  @Benchmark
  def distinctFieldsObjects(state: BenchmarkState_distinctFields, bh: Blackhole): Unit =
    doTestForce(state, bh)

  @Benchmark
  def scrollingFields(state: BenchmarkState_scrolling, bh: Blackhole): Unit =
    doTestForce(state, bh)

  @Benchmark
  def heterogeneous(state: BenchmarkState_heterogeneous, bh: Blackhole): Unit =
    doTestForce(state, bh)

  @Benchmark
  def scalarOrUndefined(state: BenchmarkState_scalarUndefined, bh: Blackhole): Unit =
    doTestForce(state, bh)

}

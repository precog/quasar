/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.impl.push

import slamdata.Predef._

import cats.data.{NonEmptyMap, NonEmptySet}
import cats.effect.IO
import cats.effect.concurrent.Deferred
import cats.implicits._

import eu.timepit.refined.auto._

import fs2.{Stream, text}
import fs2.concurrent.{Enqueue, Queue}
import fs2.job.JobManager

import quasar.api.QueryEvaluator
import quasar.api.destination.{Destination, DestinationType, ResultSink, ResultType}
import quasar.api.push.{PushMeta, RenderConfig, ResultPush, ResultPushError, ResultRender, Status}
import quasar.api.resource.ResourcePath
import quasar.api.resource.{ResourcePath, ResourceName}
import quasar.api.table.{TableColumn, TableName, TableRef}
import quasar.{ConditionMatchers, EffectfulQSpec}

import scalaz.NonEmptyList

import shims.{eqToScalaz, orderToCats, showToCats, showToScalaz}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object DefaultResultPushSpec extends EffectfulQSpec[IO] with ConditionMatchers {
  implicit val tmr = IO.timer(global)

  val Timeout = 10.seconds

  val TableId = 42
  val DestinationId = 43
  val QDestinationType = DestinationType("ref", 1L)

  type Filesystem = Map[ResourcePath, String]

  // NB: These must be at least 3 bytes each to ensure they're
  //     emitted from utf8Decode without additional input.
  val W1 = "lorum"
  val W2 = "ipsum"
  val W3 = "dolor"

  // The string formed by concatentating Ws
  val dataString = W1 + W2 + W3

  final class QDestination(q: Enqueue[IO, Option[Filesystem]]) extends Destination[IO] {
    def destinationType: DestinationType = QDestinationType

    def sinks = NonEmptyList(csvSink)

    val csvSink: ResultSink[IO] = ResultSink.csv[IO](RenderConfig.Csv()) {
      case (dst, columns, bytes) =>
        bytes.through(text.utf8Decode)
          .evalMap(s => q.enqueue1(Some(Map(dst -> s))))
          .onFinalize(q.enqueue1(None))
    }
  }

  object QDestination {
    def apply(): IO[(QDestination, Stream[IO, Filesystem])] =
      Queue.noneTerminated[IO, Filesystem] map { q =>
        (new QDestination(q), q.dequeue.scanMonoid)
      }
  }

  final class MockResultRender extends ResultRender[IO, String] {
    def renderCsv(
        input: String,
        columns: List[TableColumn],
        config: RenderConfig.Csv,
        limit: Option[Long])
        : Stream[IO, Byte] =
      Stream(input).through(text.utf8Encode)

    def renderJson(
        input: String,
        prefix: String,
        delimiter: String,
        suffix: String)
        : Stream[IO, Byte] =
      Stream.empty
  }

  def mkEvaluator(fn: String => IO[Stream[IO, String]]): QueryEvaluator[IO, String, Stream[IO, String]] =
    new QueryEvaluator[IO, String, Stream[IO, String]] {
      def evaluate(query: String): IO[Stream[IO, String]] =
        fn(query)
    }

  trait StreamControl[F[_]] {
    // Cause the stream to emit a value
    def emit(s: String): F[Unit]

    // Forcibly halts the stream
    def halt: F[Unit]

    // Completes when the stream has halted for any reason
    def halted: F[Unit]
  }

  val controlledStream: IO[(StreamControl[IO], Stream[IO, String])] =
    Queue.noneTerminated[IO, String] flatMap { q =>
      Deferred[IO, Unit] map { d =>
        val toutErr = new RuntimeException(s"Expected data stream to terminate within ${Timeout} millis.")

        val s = q.dequeue.onFinalize(d.complete(()))

        val ctl = new StreamControl[IO] {
          def emit(v: String) = q.enqueue1(Some(v))
          val halt = q.enqueue1(None)
          val halted = d.get.timeoutTo(Timeout, IO.raiseError(toutErr))
        }

        (ctl, s)
      }
    }

  val dataStream: IO[(IO[Unit], Stream[IO, String])] =
    Deferred[IO, Unit] map { d =>
      val s =
        Stream.fixedDelay[IO](100.millis)
          .zipRight(Stream(W1, W2, W3))

      (d.get, s.onFinalize(d.complete(())))
    }

  def mkResultPush(
      tables: Map[Int, TableRef[String]],
      destinations: Map[Int, Destination[IO]],
      manager: JobManager[IO, (Int, Int), Nothing],
      evaluator: QueryEvaluator[IO, String, Stream[IO, String]])
      : IO[ResultPush[IO, Int, Int]] = {

    val lookupTable: Int => IO[Option[TableRef[String]]] =
      tableId => IO(tables.get(tableId))

    val lookupDestination: Int => IO[Option[Destination[IO]]] =
      destinationId => IO(destinations.get(destinationId))

    val render = new MockResultRender

    DefaultResultPush[IO, Int, Int, String, String](
      lookupTable,
      evaluator,
      lookupDestination,
      manager,
      render)
  }

  val jobManager =
    JobManager[IO, (Int, Int), Nothing]()

  def awaitFs(fss: Stream[IO, Filesystem], count: Long = -1): IO[Filesystem] = {
    val (s, msg) =
      if (count <= 0)
        (fss, "final filesystem")
      else
        (fss.take(count + 1), s"${count} filesystem updates")

    s.compile.lastOrError.timeoutTo(
      Timeout,
      IO.raiseError(new RuntimeException(s"Expected $msg within ${Timeout}.")))
  }

  def await(io: IO[_]): IO[Unit] =
    io.timeoutTo(Timeout, IO.raiseError(new RuntimeException(s"Expected completion within ${Timeout}."))).void

  def awaitStatusLike(
      p: ResultPush[IO, Int, Int],
      tableId: Int,
      destinationId: Int)(
      f: PartialFunction[PushMeta, Boolean])
      : IO[PushMeta] = {

    val metas =
      Stream.eval(p.destinationStatus(destinationId))
        .map(_.toOption.flatMap(_.get(tableId)))
        .unNone

    Stream.fixedDelay[IO](100.millis)
      .zipRight(metas)
      .filter(f.applyOrElse(_, (_: PushMeta) => false))
      .take(1)
      .compile.lastOrError
      .timeoutTo(Timeout, IO.raiseError(new RuntimeException(s"Expected a matching `PushMeta` $Timeout.")))
  }

  "start" >> {
    "asynchronously pushes a table to a destination" >>* {
      val pushPath = ResourcePath.root() / ResourceName("foo") / ResourceName("bar")
      val query = "query"
      val testTable = TableRef(TableName("foo"), query, List())

      jobManager use { jm =>
        for {
          (destination, filesystem) <- QDestination()

          (halted, data) <- dataStream

          evaluator = mkEvaluator(_ => IO(data))

          push <- mkResultPush(Map(TableId -> testTable), Map(DestinationId -> destination), jm, evaluator)

          startRes <- push.start(TableId, DestinationId, pushPath, ResultType.Csv, None)

          _ <- await(halted)

          filesystemAfterPush <- awaitFs(filesystem)
        } yield {
          filesystemAfterPush.keySet must equal(Set(pushPath))
          filesystemAfterPush(pushPath) must equal(dataString)
          startRes must beNormal
        }
      }
    }

    "rejects an already running push of a table to the same destination" >>* {
      val path = ResourcePath.root()
      val testTable = TableRef(TableName("baz"), "query", List())

      jobManager use { jm =>
        for {
          (destination, _) <- QDestination()
          (_, data) <- controlledStream

          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator(_ => IO(data)))

          firstStartStatus <- push.start(TableId, DestinationId, path, ResultType.Csv, None)
          secondStartStatus <- push.start(TableId, DestinationId, path, ResultType.Csv, None)
        } yield {
          firstStartStatus must beNormal
          secondStartStatus must beAbnormal(ResultPushError.PushAlreadyRunning(TableId, DestinationId))
        }
      }
    }

    "allows concurrent push of a table to multiple destinations" >>* {
      val path = ResourcePath.root()
      val testTable = TableRef(TableName("baz"), "query", List())
      val dest2 = DestinationId + 1

      jobManager use { jm =>
        for {
          (destination, _) <- QDestination()

          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination, dest2 -> destination),
            jm,
            mkEvaluator(_ => controlledStream.map(_._2)))

          firstStartStatus <- push.start(TableId, DestinationId, path, ResultType.Csv, None)
          secondStartStatus <- push.start(TableId, dest2, path, ResultType.Csv, None)
        } yield {
          firstStartStatus must beNormal
          secondStartStatus must beNormal
        }
      }
    }

    "fails with 'destination not found' when destination doesn't exist" >>* {
      val path = ResourcePath.root()
      val testTable = TableRef(TableName("baz"), "query", List())

      jobManager use { jm =>
        for {
          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(),
            jm,
            mkEvaluator(_ => IO(Stream())))
          startStatus <- push.start(TableId, DestinationId, path, ResultType.Csv, None)
        } yield {
          startStatus must beAbnormal(ResultPushError.DestinationNotFound(DestinationId))
        }
      }
    }

    "fails with 'table not found' when table doesn't exist" >>* {
      val path = ResourcePath.root()
      val testTable = TableRef(TableName("baz"), "query", List())

      jobManager use { jm =>
        for {
          (dest, _) <- QDestination()
          push <- mkResultPush(
            Map(),
            Map(DestinationId -> dest),
            jm,
            mkEvaluator(_ => IO(Stream())))
          startStatus <- push.start(TableId, DestinationId, path, ResultType.Csv, None)
        } yield {
          startStatus must beAbnormal(ResultPushError.TableNotFound(TableId))
        }
      }
    }

    "fails when destination doesn't support requested format" >> todo
  }

  "start these" >> {
    val path1 = ResourcePath.root() / ResourceName("foo")
    val testTable1 = TableRef(TableName("foo"), "queryFoo", List())

    val path2 = ResourcePath.root() / ResourceName("bar")
    val testTable2 = TableRef(TableName("bar"), "queryBar", List())

    "starts a push for each table to the destination" >>* {
      jobManager use { jm =>
        for {
          (destination, filesystem) <- QDestination()

          (fooHalted, dataFoo) <- dataStream
          (barHalted, dataBar) <- dataStream

          push <- mkResultPush(
            Map(1 -> testTable1, 2 -> testTable2),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator {
              case "queryFoo" => IO(dataFoo)
              case "queryBar" => IO(dataBar)
            })

          errors <- push.startThese(DestinationId, NonEmptyMap.of(
            1 -> ((path1, ResultType.Csv, None)),
            2 -> ((path2, ResultType.Csv, None))))

          _ <- await(fooHalted)
          _ <- await(barHalted)

        } yield {
          errors.isEmpty must beTrue
        }
      }
    }

    "returns the cause of any pushes that failed to start" >>* {
      jobManager use { jm =>
        for {
          (destination, filesystem) <- QDestination()

          (barHalted, barData) <- dataStream

          push <- mkResultPush(
            Map(2 -> testTable2),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator {
              case "queryBar" => IO(barData)
            })

          errors <- push.startThese(DestinationId, NonEmptyMap.of(
            1 -> ((path1, ResultType.Csv, None)),
            2 -> ((path2, ResultType.Csv, None))))

          _ <- await(barHalted)
        } yield {
          errors must_=== Map(1 -> ResultPushError.TableNotFound(1))
        }
      }
    }
  }

  "cancel" >> {
    "aborts a running push" >>* {
      val pushPath = ResourcePath.root() / ResourceName("foo") / ResourceName("bar")
      val query = "query"
      val testTable = TableRef(TableName("foo"), query, List())

      jobManager use { jm =>
        for {
          (destination, filesystem) <- QDestination()

          (ctl, data) <- controlledStream

          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator(_ => IO(data)))

          startRes <- push.start(TableId, DestinationId, pushPath, ResultType.Csv, None)

          _ <- ctl.emit(W1)

          filesystemAfterPush <- awaitFs(filesystem, 1)

          cancelRes <- push.cancel(TableId, DestinationId)

          _ <- await(ctl.halted)
        } yield {
          filesystemAfterPush.keySet must equal(Set(pushPath))
          // check if a *partial* result was pushed
          filesystemAfterPush(pushPath) must equal(W1)
          startRes must beNormal
          cancelRes must beNormal
        }
      }
    }

    "no-op when push already completed" >>* {
      val testTable = TableRef(TableName("foo"), "query", List())

      jobManager use { jm =>
        for {
          (destination, filesystem) <- QDestination()

          (halted, data) <- dataStream

          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator(_ => IO(data)))

          startRes <- push.start(TableId, DestinationId, ResourcePath.root(), ResultType.Csv, None)

          _ <- awaitFs(filesystem)
          _ <- await(halted)

          cancelRes <- push.cancel(TableId, DestinationId)
        } yield {
          startRes must beNormal
          cancelRes must beNormal
        }
      }
    }

    "no-op when no push for extant table" >>* {
      val testTable = TableRef(TableName("foo"), "query", List())

      jobManager use { jm =>
        for {
          (destination, _) <- QDestination()

          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator(_ => IO(Stream())))

          cancelRes <- push.cancel(TableId, DestinationId)
        } yield {
          cancelRes must beNormal
        }
      }
    }

    "returns 'destination not found' when destination doesn't exist" >>* {
      val testTable = TableRef(TableName("foo"), "query", List())

      jobManager use { jm =>
        for {
          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(),
            jm,
            mkEvaluator(_ => IO(Stream())))

          cancelRes <- push.cancel(TableId, DestinationId)
        } yield {
          cancelRes must beAbnormal(ResultPushError.DestinationNotFound(DestinationId))
        }
      }
    }

    "returns 'table not found' when table doesn't exist" >>* {
      jobManager use { jm =>
        for {
          (dest, _) <- QDestination()

          push <- mkResultPush(
            Map(),
            Map(DestinationId -> dest),
            jm,
            mkEvaluator(_ => IO(Stream())))

          cancelRes <- push.cancel(TableId, DestinationId)
        } yield {
          cancelRes must beAbnormal(ResultPushError.TableNotFound(TableId))
        }
      }
    }
  }

  "cancel these" >> {
    "cancels running pushes" >>* {
      val queryA = "queryA"
      val queryB = "queryB"
      val queryC = "queryC"

      val testTableA = TableRef(TableName("A"), queryA, List())
      val testTableB = TableRef(TableName("B"), queryB, List())
      val testTableC = TableRef(TableName("C"), queryC, List())

      val pushPathA = ResourcePath.root() / ResourceName("foo") / ResourceName("A")
      val pushPathB = ResourcePath.root() / ResourceName("foo") / ResourceName("B")

      val idA = TableId
      val idB = idA + 1
      val idC = idB + 1

      jobManager use { jm =>
        for {
          (destination, filesystem) <- QDestination()

          (ctlA, dataA) <- controlledStream
          (ctlB, dataB) <- controlledStream

          push <- mkResultPush(
            Map(
              idA -> testTableA,
              idB -> testTableB,
              idC -> testTableC),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator {
              case `queryA` => IO(dataA)
              case `queryB` => IO(dataB)
            })

          startRes <- push.startThese(
            DestinationId,
            NonEmptyMap.of(
              idA -> ((pushPathA, ResultType.Csv, None)),
              idB -> ((pushPathB, ResultType.Csv, None))))

          _ <- ctlA.emit(W1)
          _ <- ctlB.emit(W2)

          filesystemAfterPush <- awaitFs(filesystem, 2)

          cancelRes <- push.cancelThese(
            DestinationId,
            NonEmptySet.of(idA, idB, idC))

          // fail the test if push evaluation was not cancelled
          _ <- await(ctlA.halted)
          _ <- await(ctlB.halted)
        } yield {
          filesystemAfterPush.keySet must equal(Set(pushPathA, pushPathB))
          // check if a *partial* result was pushed
          filesystemAfterPush(pushPathA) must equal(W1)
          filesystemAfterPush(pushPathB) must equal(W2)
          startRes.isEmpty must beTrue
          cancelRes.isEmpty must beTrue
        }
      }
    }

    "returns the cause of any push that failed to cancel" >>* {
      val queryA = "queryA"
      val testTableA = TableRef(TableName("A"), queryA, List())
      val pushPathA = ResourcePath.root() / ResourceName("foo") / ResourceName("A")

      val idA = TableId
      val idB = idA + 1

      jobManager use { jm =>
        for {
          (destination, filesystem) <- QDestination()

          (ctlA, dataA) <- controlledStream

          push <- mkResultPush(
            Map(idA -> testTableA),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator {
              case `queryA` => IO(dataA)
            })

          startRes <- push.start(
            idA,
            DestinationId,
            pushPathA,
            ResultType.Csv,
            None)

          _ <- ctlA.emit(W1)

          filesystemAfterPush <- awaitFs(filesystem, 1)

          cancelRes <- push.cancelThese(DestinationId, NonEmptySet.of(idA, idB))

          _ <- await(ctlA.halted)
        } yield {
          filesystemAfterPush.keySet must equal(Set(pushPathA))
          // check if a *partial* result was pushed
          filesystemAfterPush(pushPathA) must equal(W1)
          startRes must beNormal
          cancelRes must_=== Map(idB -> ResultPushError.TableNotFound(idB))
        }
      }
    }
  }

  "destination status" >> {
    "empty when nothing has been pushed" >>* {
      val testTable = TableRef(TableName("baz"), "query", List())

      jobManager use { jm =>
        for {
          (destination, _) <- QDestination()

          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator(_ => IO(Stream())))

          pushStatus <- push.destinationStatus(DestinationId)
        } yield {
          pushStatus must beLike {
            case Right(m) => m.isEmpty must beTrue
          }
        }
      }
    }

    "includes running push" >>* {
      val testTable = TableRef(TableName("baz"), "query", List())

      jobManager use { jm =>
        for {
          (destination, filesystem) <- QDestination()

          (ctl, data) <- controlledStream

          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator(_ => IO(data)))

          _ <- push.start(TableId, DestinationId, ResourcePath.root(), ResultType.Csv, None)

          _ <- ctl.emit(W1)
          _ <- awaitFs(filesystem, 1)

          _ <- awaitStatusLike(push, TableId, DestinationId) {
            case PushMeta(_, ResultType.Csv, None, Status.Running(_)) => true
          }
        } yield ok
      }
    }

    "includes canceled push" >>* {
      val testTable = TableRef(TableName("baz"), "query", List())

      jobManager use { jm =>
        for {
          (destination, filesystem) <- QDestination()

          (ctl, data) <- controlledStream

          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator(_ => IO(data)))

          _ <- push.start(TableId, DestinationId, ResourcePath.root(), ResultType.Csv, None)

          _ <- ctl.emit(W1)
          _ <- awaitFs(filesystem, 1)

          _ <- push.cancel(TableId, DestinationId)

          _ <- await(ctl.halted)

          _ <- awaitStatusLike(push, TableId, DestinationId) {
            case PushMeta(_, ResultType.Csv, None, Status.Canceled(_, _)) => true
          }
        } yield ok
      }
    }

    "includes completed push" >>* {
      val testTable = TableRef(TableName("baz"), "query", List())

      jobManager use { jm =>
        for {
          (destination, filesystem) <- QDestination()

          (halted, data) <- dataStream

          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator(_ => IO(data)))

          _ <- push.start(TableId, DestinationId, ResourcePath.root(), ResultType.Csv, None)
          _ <- await(halted)

          _ <- awaitStatusLike(push, TableId, DestinationId) {
            case PushMeta(_, ResultType.Csv, None, Status.Finished(_, _)) => true
          }
        } yield ok
      }
    }

    "includes pushes that failed to initialize along with error that led to failure" >>* {
      val testTable = TableRef(TableName("foo"), "query", List())

      jobManager use { jm =>
        for {
          (destination, filesystem) <- QDestination()

          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator(_ => IO.raiseError[Stream[IO, String]](new Exception("boom"))))

          pushStatus <- push.start(TableId, DestinationId, ResourcePath.root(), ResultType.Csv, None)

          status <- awaitStatusLike(push, TableId, DestinationId) {
            case PushMeta(_, _, _, Status.Failed(_, _, _)) => true
          }
        } yield {
          status must beLike {
            case PushMeta(_, ResultType.Csv, None, Status.Failed(ex, _, _)) =>
              ex.getMessage must equal("boom")
          }
        }
      }
    }

    "includes pushes that failed during streaming along with the error that led to falure" >>* {
      val testTable = TableRef(TableName("baz"), "query", List())
      val ex = new Exception("boom")

      jobManager use { jm =>
        for {
          (destination, filesystem) <- QDestination()

          (halted, data) <- dataStream

          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator(_ => IO(data ++ Stream.raiseError[IO](ex))))

          _ <- push.start(TableId, DestinationId, ResourcePath.root(), ResultType.Csv, None)
          _ <- await(halted)

          status <- awaitStatusLike(push, TableId, DestinationId) {
            case PushMeta(_, _, _, Status.Failed(_, _, _)) => true
          }
        } yield {
          status must beLike {
            case PushMeta(_, ResultType.Csv, None, Status.Failed(ex, _, _)) =>
              ex.getMessage must equal("boom")
          }
        }
      }
    }

    "returns 'destination not found' when destination doesn't exist" >>* {
      val testTable = TableRef(TableName("baz"), "query", List())

      jobManager use { jm =>
        for {
          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(),
            jm,
            mkEvaluator(_ => IO(Stream())))
          pushStatus <- push.destinationStatus(DestinationId)
        } yield {
          pushStatus must beLeft(ResultPushError.DestinationNotFound(DestinationId))
        }
      }
    }
  }

  "cancel all" >> {
    "aborts all running pushes" >>* {
      val path1 = ResourcePath.root() / ResourceName("foo")
      val testTable1 = TableRef(TableName("foo"), "queryFoo", List())

      val path2 = ResourcePath.root() / ResourceName("bar")
      val testTable2 = TableRef(TableName("bar"), "queryBar", List())

      jobManager use { jm =>
        for {
          (destination, filesystem) <- QDestination()

          (ctlA, dataA) <- controlledStream
          (ctlB, dataB) <- controlledStream

          push <- mkResultPush(
            Map(1 -> testTable1, 2 -> testTable2),
            Map(DestinationId -> destination),
            jm,
            mkEvaluator {
              case "queryFoo" => IO(dataA)
              case "queryBar" => IO(dataB)
            })

          _ <- push.start(1, DestinationId, path1, ResultType.Csv, None)
          _ <- push.start(2, DestinationId, path2, ResultType.Csv, None)

          _ <- ctlA.emit(W1)
          _ <- ctlB.emit(W2)

          _ <- awaitFs(filesystem, 2)

          _ <- push.cancelAll

          _ <- await(ctlA.halted)
          _ <- await(ctlB.halted)
        } yield ok
      }
    }

    "no-op when no running pushes" >>* {
      jobManager use { jm =>
        for {
          push <- mkResultPush(
            Map(),
            Map(),
            jm,
            mkEvaluator(_ => IO(Stream())))
          _ <- push.cancelAll
        } yield ok
      }
    }
  }
}

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

package quasar.impl.push

import slamdata.Predef._

import cats.data.{Ior, NonEmptyList, NonEmptyMap, NonEmptySet}
import cats.effect.{IO, Resource}
import cats.effect.concurrent.Deferred
import cats.implicits._

import eu.timepit.refined.auto._

import fs2.{Stream, text}
import fs2.concurrent.{Enqueue, Queue}
import fs2.job.JobManager

import monocle.Prism

import quasar.{Condition, ConditionMatchers, EffectfulQSpec}
import quasar.api.{Column, ColumnType, Label, Labeled, QueryEvaluator}
import quasar.api.destination._
import quasar.api.push._
import quasar.api.push.param._
import quasar.api.resource.ResourcePath
import quasar.api.resource.{ResourcePath, ResourceName}
import quasar.api.table.{TableColumn, TableName, TableRef}
import quasar.connector.{ActualKey, DataEvent, IdType, FormalKey}
import quasar.connector.destination._
import quasar.connector.render._

import shims.{eqToScalaz, orderToCats, showToCats, showToScalaz}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import skolems.∃

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
    sealed trait Constructor[P] extends ConstructorLike[P] with Product with Serializable

    sealed trait Type extends Product with Serializable

    case object Bool extends Type

    case class Varchar(length: Int) extends Type
    case object Varchar extends Constructor[Int]

    case class Num(width: Int) extends Type
    case object Num extends Constructor[Int]

    sealed trait TypeId extends Product with Serializable
    case object BoolId extends TypeId
    case object VarcharId extends TypeId
    case object NumId extends TypeId

    val typeIdOrdinal =
      Prism.partial[Int, TypeId] {
        case 0 => BoolId
        case 1 => VarcharId
        case 2 => NumId
      } {
        case BoolId => 0
        case VarcharId => 1
        case NumId => 2
      }

    val typeIdLabel =
      Label label {
        case BoolId => "BOOLEAN"
        case VarcharId => "VARCHAR"
        case NumId => "NUMBER"
      }

    def coerce(s: ColumnType.Scalar): TypeCoercion[TypeId] =
      s match {
        case ColumnType.Boolean => TypeCoercion.Satisfied(NonEmptyList.one(BoolId))
        case ColumnType.String => TypeCoercion.Satisfied(NonEmptyList.one(VarcharId))
        case ColumnType.Number => TypeCoercion.Satisfied(NonEmptyList.one(NumId))
        case other => TypeCoercion.Unsatisfied(Nil, None)
      }

    def construct(id: TypeId): Either[Type, ∃[λ[α => (Constructor[α], Labeled[Formal[α]])]]] =
      id match {
        case BoolId =>
          Left(Bool)

        case VarcharId =>
          Right(formalConstructor(
            Varchar,
            "Length",
            Formal.integer(Some(Ior.both(1, 256)), None)))

        case NumId =>
          Right(formalConstructor(
            Num,
            "Width",
            Formal.enum[Int](
              "4-bits" -> 4,
              "8-bits" -> 8,
              "16-bits" -> 16)))
      }

    def destinationType: DestinationType = QDestinationType

    def sinks = NonEmptyList.one(csvSink)

    val csvSink: ResultSink[IO, Type] = ResultSink.create[IO, Type](RenderConfig.Csv()) {
      case (dst, _, bytes) =>
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
    def render(
        input: String,
        columns: List[TableColumn],
        config: RenderConfig,
        limit: Option[Long])
        : Stream[IO, Byte] =
      Stream(input).through(text.utf8Encode)

    def renderUpserts[A](
        input: Input[String],
        idColumn: Column[IdType],
        offsetColumn: Column[FormalKey[Unit, A]],
        renderedColumns: NonEmptyList[TableColumn],
        config: RenderConfig.Csv,
        limit: Option[Long])
        : Stream[IO, DataEvent[ActualKey[A]]] = ???
    }

  def mkEvaluator(fn: String => IO[Stream[IO, String]]): QueryEvaluator[Resource[IO, ?], String, Stream[IO, String]] =
    QueryEvaluator(fn).mapF(Resource.liftF(_))

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
      evaluator: QueryEvaluator[Resource[IO, ?], String, Stream[IO, String]])
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
        .repeat

    Stream.fixedDelay[IO](100.millis)
      .zipRight(metas)
      .filter(f.applyOrElse(_, (_: PushMeta) => false))
      .take(1)
      .compile.lastOrError
      .timeoutTo(Timeout, IO.raiseError(new RuntimeException(s"Expected a matching `PushMeta` $Timeout.")))
  }

  val colX = NonEmptyList.one(Column("X", SelectedType(TypeIndex(0), None)))

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

          startRes <- push.start(TableId, colX, DestinationId, pushPath, ResultType.Csv, None)

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

          firstStartStatus <- push.start(TableId, colX, DestinationId, path, ResultType.Csv, None)
          secondStartStatus <- push.start(TableId, colX, DestinationId, path, ResultType.Csv, None)
        } yield {
          firstStartStatus must beNormal
          secondStartStatus must beAbnormal(NonEmptyList.one(ResultPushError.PushAlreadyRunning(TableId, DestinationId)))
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

          firstStartStatus <- push.start(TableId, colX, DestinationId, path, ResultType.Csv, None)
          secondStartStatus <- push.start(TableId, colX, dest2, path, ResultType.Csv, None)
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
          startStatus <- push.start(TableId, colX, DestinationId, path, ResultType.Csv, None)
        } yield {
          startStatus must beAbnormal(NonEmptyList.one(ResultPushError.DestinationNotFound(DestinationId)))
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
          startStatus <- push.start(TableId, colX, DestinationId, path, ResultType.Csv, None)
        } yield {
          startStatus must beAbnormal(NonEmptyList.one(ResultPushError.TableNotFound(TableId)))
        }
      }
    }

    "fails when destination doesn't support requested format" >> todo

    "fails when selected type isn't found" >>* {
      val path = ResourcePath.root()
      val testTable = TableRef(TableName("baz"), "query", List())

      jobManager use { jm =>
        for {
          (dest, _) <- QDestination()

          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> dest),
            jm,
            mkEvaluator(_ => IO(Stream())))

          idx = TypeIndex(-1)
          cols = NonEmptyList.one(Column("A", SelectedType(idx, None)))

          startStatus <- push.start(TableId, cols, DestinationId, path, ResultType.Csv, None)
        } yield {
          startStatus must beAbnormal(NonEmptyList.one(ResultPushError.TypeNotFound(DestinationId, "A", idx)))
        }
      }
    }

    "fails when a required type argument is missing" >>* {
      val path = ResourcePath.root()
      val testTable = TableRef(TableName("baz"), "query", List())

      jobManager use { jm =>
        for {
          (dest, _) <- QDestination()

          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> dest),
            jm,
            mkEvaluator(_ => IO(Stream())))

          idx = TypeIndex(1)
          cols = NonEmptyList.one(Column("A", SelectedType(idx, None)))

          startStatus <- push.start(TableId, cols, DestinationId, path, ResultType.Csv, None)
        } yield {
          startStatus must beLike {
            case Condition.Abnormal(NonEmptyList(ResultPushError.TypeConstructionFailed(
              DestinationId,
              "A",
              "VARCHAR",
              NonEmptyList(ParamError.ParamMissing("Length", _), Nil)), Nil)) => ok
          }
        }
      }
    }

    "fails when wrong kind of type argument given" >>* {
      val path = ResourcePath.root()
      val testTable = TableRef(TableName("baz"), "query", List())

      jobManager use { jm =>
        for {
          (dest, _) <- QDestination()

          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> dest),
            jm,
            mkEvaluator(_ => IO(Stream())))

          idx = TypeIndex(1)
          cols = NonEmptyList.one(Column("A", SelectedType(idx, Some(∃(Actual.boolean(false))))))

          startStatus <- push.start(TableId, cols, DestinationId, path, ResultType.Csv, None)
        } yield {
          startStatus must beLike {
            case Condition.Abnormal(NonEmptyList(ResultPushError.TypeConstructionFailed(
              DestinationId,
              "A",
              "VARCHAR",
              NonEmptyList(ParamError.ParamMismatch("Length", _, _), Nil)), Nil)) => ok
          }
        }
      }
    }

    "fails when integer type argument is out of bounds" >>* {
      val path = ResourcePath.root()
      val testTable = TableRef(TableName("baz"), "query", List())

      jobManager use { jm =>
        for {
          (dest, _) <- QDestination()

          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> dest),
            jm,
            mkEvaluator(_ => IO(Stream())))

          idx = TypeIndex(1)

          colsLow = NonEmptyList.one(Column("A", SelectedType(idx, Some(∃(Actual.integer(0))))))
          startLow <- push.start(TableId, colsLow, DestinationId, path, ResultType.Csv, None)

          colsHigh = NonEmptyList.one(Column("A", SelectedType(idx, Some(∃(Actual.integer(300))))))
          startHigh <- push.start(TableId, colsHigh, DestinationId, path, ResultType.Csv, None)
        } yield {
          startLow must beLike {
            case Condition.Abnormal(NonEmptyList(ResultPushError.TypeConstructionFailed(
              DestinationId,
              "A",
              "VARCHAR",
              NonEmptyList(ParamError.IntOutOfBounds("Length", 0, Ior.Both(1, 256)), Nil)), Nil)) => ok
          }

          startHigh must beLike {
            case Condition.Abnormal(NonEmptyList(ResultPushError.TypeConstructionFailed(
              DestinationId,
              "A",
              "VARCHAR",
              NonEmptyList(ParamError.IntOutOfBounds("Length", 300, Ior.Both(1, 256)), Nil)), Nil)) => ok
          }
        }
      }
    }

    "fails when enum type argument selection is not found" >>* {
      val path = ResourcePath.root()
      val testTable = TableRef(TableName("baz"), "query", List())

      jobManager use { jm =>
        for {
          (dest, _) <- QDestination()

          push <- mkResultPush(
            Map(TableId -> testTable),
            Map(DestinationId -> dest),
            jm,
            mkEvaluator(_ => IO(Stream())))

          idx = TypeIndex(2)
          cols = NonEmptyList.one(Column("B", SelectedType(idx, Some(∃(Actual.enumSelect("32-bits"))))))

          startStatus <- push.start(TableId, cols, DestinationId, path, ResultType.Csv, None)
        } yield {
          startStatus must beLike {
            case Condition.Abnormal(NonEmptyList(ResultPushError.TypeConstructionFailed(
              DestinationId,
              "B",
              "NUMBER",
              NonEmptyList(ParamError.ValueNotInEnum("Width", "32-bits", xs), Nil)), Nil)) =>
                xs must_=== NonEmptySet.of("4-bits", "8-bits", "16-bits")
          }
        }
      }
    }
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
            1 -> ((colX, path1, ResultType.Csv, None)),
            2 -> ((colX, path2, ResultType.Csv, None))))

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
            1 -> ((colX, path1, ResultType.Csv, None)),
            2 -> ((colX, path2, ResultType.Csv, None))))

          _ <- await(barHalted)
        } yield {
          errors must_=== Map(1 -> NonEmptyList.one(ResultPushError.TableNotFound(1)))
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

          startRes <- push.start(TableId, colX, DestinationId, pushPath, ResultType.Csv, None)

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

          startRes <- push.start(TableId, colX, DestinationId, ResourcePath.root(), ResultType.Csv, None)

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
              idA -> ((colX, pushPathA, ResultType.Csv, None)),
              idB -> ((colX, pushPathB, ResultType.Csv, None))))

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
            colX,
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

          _ <- push.start(TableId, colX, DestinationId, ResourcePath.root(), ResultType.Csv, None)

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

          _ <- push.start(TableId, colX, DestinationId, ResourcePath.root(), ResultType.Csv, None)

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

          _ <- push.start(TableId, colX, DestinationId, ResourcePath.root(), ResultType.Csv, None)
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

          pushStatus <- push.start(TableId, colX, DestinationId, ResourcePath.root(), ResultType.Csv, None)

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

          _ <- push.start(TableId, colX, DestinationId, ResourcePath.root(), ResultType.Csv, None)
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

          _ <- push.start(1, colX, DestinationId, path1, ResultType.Csv, None)
          _ <- push.start(2, colX, DestinationId, path2, ResultType.Csv, None)

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

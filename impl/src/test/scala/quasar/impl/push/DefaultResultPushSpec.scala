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

import cats.data.{Ior, NonEmptyList, NonEmptySet}
import cats.derived.auto.order._
import cats.effect.{Blocker, IO, Resource}
import cats.effect.concurrent.{Deferred, Ref}
import cats.implicits._

import fs2.{Pipe, Stream, text, Pull}
import fs2.concurrent.Queue

import java.time.{Instant, ZoneOffset}

import monocle.Prism

import monocle.Traversal

import org.specs2.execute.AsResult
import org.specs2.specification.core.Fragment

import quasar.{ConditionMatchers, EffectfulQSpec}
import quasar.api.{Column, ColumnType, DataPathSegment, Label, Labeled, QueryEvaluator}
import quasar.api.destination._
import quasar.api.push._
import quasar.api.push.param._
import quasar.api.resource.ResourcePath
import quasar.api.resource.{ResourcePath, ResourceName}
import quasar.connector.{AppendEvent, DataEvent, Offset}
import quasar.connector.destination._
import quasar.connector.render._
import quasar.contrib.scalaz.MonadError_
import quasar.impl.storage
import quasar.impl.storage.{StoreError, RefIndexedStore}

import shims.{applicativeToCats, eqToScalaz, orderToCats, orderToScalaz, showToCats, showToScalaz}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import scalaz.IMap

import scodec.Codec
import scodec.codecs._

import shapeless._

import skolems.{∃, ∀}

import spire.math.Real

object DefaultResultPushSpec extends EffectfulQSpec[IO] with ConditionMatchers {
  import ResultPushError._

  addSections

  implicit val intCodec: Codec[Int] = int32
  implicit val resourcePathCodec: Codec[ResourcePath] = SerializableStore.codec[ResourcePath]

  implicit val ioMonadStoreError: MonadError_[IO, StoreError] =
    MonadError_.facet[IO](StoreError.throwableP)

  implicit val tmr = IO.timer(global)

  val Timeout = 10.seconds

  val DestinationId = 43
  val QDestinationType = DestinationType("ref", 1)

  type Filesystem = Map[ResourcePath, String]

  // NB: These must be at least 3 bytes each to ensure they're
  //     emitted from utf8Decode without additional input.
  val W1 = "lorum"
  val W2 = "ipsum"
  val W3 = "dolor"

  // The string formed by concatentating Ws
  val dataString = W1 + W2 + W3

  final class QDestination(fs: Ref[IO, Filesystem], support: QDestination.Support)
      extends Destination[IO] {

    import QDestination._

    sealed trait Type extends Product with Serializable
    sealed trait TypeId extends Product with Serializable

    case object Bool extends Type with TypeId

    case class Varchar(length: Int) extends Type
    case object Varchar extends TypeId

    case class Num(width: Int) extends Type
    case object Num extends TypeId

    val typeIdOrdinal =
      Prism.partial[Int, TypeId] {
        case 0 => Bool
        case 1 => Varchar
        case 2 => Num
      } {
        case Bool => 0
        case Varchar => 1
        case Num => 2
      }

    val typeIdLabel =
      Label label {
        case Bool => "BOOLEAN"
        case Varchar => "VARCHAR"
        case Num => "NUMBER"
      }

    def coerce(s: ColumnType.Scalar): TypeCoercion[TypeId] =
      s match {
        case ColumnType.Boolean => TypeCoercion.Satisfied(NonEmptyList.one(Bool))
        case ColumnType.String => TypeCoercion.Satisfied(NonEmptyList.one(Varchar))
        case ColumnType.Number => TypeCoercion.Satisfied(NonEmptyList.one(Num))
        case other => TypeCoercion.Unsatisfied(Nil, None)
      }

    def construct(id: TypeId): Either[Type, Constructor[Type]] =
      id match {
        case Bool =>
          Left(Bool)

        case Varchar =>
          Right(Constructor.Unary(
            Labeled("Length", Formal.integer(Some(Ior.both(1, 256)), None, None)),
            Varchar(_)))

        case Num =>
          Right(Constructor.Unary(
            Labeled(
              "Width",
              Formal.enum[Int](
                "4-bits" -> 4,
                "8-bits" -> 8,
                "16-bits" -> 16)),
            Num(_)))
      }

    def destinationType: DestinationType = QDestinationType

    def sinks = support match {
      case Create => NonEmptyList.one(createSink)
      case Upsert => NonEmptyList.one(upsertSink)
      case Append => NonEmptyList.one(appendSink)
      case All => NonEmptyList.of(createSink, upsertSink, appendSink)
    }

    val createSink: ResultSink[IO, Type] =
      ResultSink.create { (dst, _) =>
        val pipe = (in: Stream[IO, Byte]) =>
          (Stream.eval_(fs.update(_.updated(dst, ""))) ++ in)
            .through(text.utf8Decode)
            .evalMap(s => fs.update(_ |+| Map(dst -> s)))

        (RenderConfig.Csv(), pipe)
      }

    val upsertSink: ResultSink[IO, Type] =
      ResultSink.upsert[IO, Type, Byte] { args =>
        val init = args.writeMode match {
          case WriteMode.Replace =>
            Stream.eval_(fs.update(_.updated(args.path, "")))

          case WriteMode.Append =>
            Stream.empty
        }

        def pipe[A](dataEvents: Stream[IO, DataEvent[Byte, OffsetKey.Actual[A]]])
            : Stream[IO, OffsetKey.Actual[A]] =
          (init ++ dataEvents) flatMap {
            case DataEvent.Create(c) =>
              Stream.chunk(c)
                .through(text.utf8Decode)
                .evalMap(s => fs.update(_ |+| Map(args.path -> s)))
                .drain

            case DataEvent.Commit(k) =>
              Stream.emit(k)

            case _ =>
              Stream.empty
          }

        (RenderConfig.Csv(),
          ∀[λ[α => Pipe[IO, DataEvent[Byte, OffsetKey.Actual[α]], OffsetKey.Actual[α]]]](pipe))
      }
    val appendSink: ResultSink[IO, Type] =
      ResultSink.append[IO, Type, Byte] { (path, columns) =>
        def pipe[A]: Pipe[IO, AppendEvent[Byte, OffsetKey.Actual[A]], OffsetKey.Actual[A]] = _.flatMap {
          case DataEvent.Create(c) =>
            Stream.chunk(c)
                .through(text.utf8Decode)
                .evalMap(s => fs.update(_ |+| Map(path -> s)))
                .drain
            case DataEvent.Commit(k) =>
              Stream.emit(k)
        }
        (RenderConfig.Csv(),
          ∀[λ[α => Pipe[IO, AppendEvent[Byte, OffsetKey.Actual[α]], OffsetKey.Actual[α]]]](pipe))
      }
  }

  object QDestination {
    sealed trait Support
    object Create extends Support
    object Upsert extends Support
    object Append extends Support
    object All extends Support

    def apply(supports: Support = All): IO[(QDestination, IO[Filesystem])] =
      Ref.of[IO, Filesystem](Map()) map { fs =>
        (new QDestination(fs, supports), fs.get)
      }
  }

  final class MockResultRender extends ResultRender[IO, Stream[IO, String]] {
    def render[A](
        input: Stream[IO, String],
        columns: NonEmptyList[Column[ColumnType.Scalar]],
        config: RenderConfig[A],
        rowLimit: Option[Long])
        : Stream[IO, A] = {

      val limited = rowLimit.fold(input)(input.take(_))

      config match {
        case _: RenderConfig.Csv => limited.through(text.utf8Encode)
        case _: RenderConfig.Json => limited.through(text.utf8Encode)
        case _: RenderConfig.Separated => limited
      }
    }

    def renderUpserts[A, P](
        input: RenderInput[Stream[IO, String]],
        idColumn: Column[IdType],
        offsetColumn: Column[OffsetKey.Formal[Unit, A]],
        renderedColumns: NonEmptyList[Column[ColumnType.Scalar]],
        config: RenderConfig[P],
        limit: Option[Long])
        : Stream[IO, DataEvent[P, OffsetKey.Actual[A]]] = {
      val creates: Stream[IO, DataEvent.Create[P]] =
        config match {
          case (r: RenderConfig.Csv) =>
            limit.fold(input.value)(input.value.take(_))
              .through(text.utf8Encode)
              .chunks
              .map(DataEvent.Create(_))

          case (r: RenderConfig.Separated) =>
            limit.fold(input.value)(input.value.take(_))
              .chunks
              .map(DataEvent.Create(_))

          case _ =>
            Stream.raiseError[IO](new RuntimeException("renderJson not implemented"))
        }

      offsetColumn.tpe match {
        case OffsetKey.RealKey(_) =>
          // emits offsets equal to the number of bytes emitted
          creates
            .scan((0, Stream.empty: Stream[IO, DataEvent[P, OffsetKey.Actual[A]]])) {
              case ((acc, _), c) =>
                val total = acc + c.records.size
                val out = Stream(c, DataEvent.Commit(OffsetKey.Actual.real(total)))

                (total, out)
            }
            .flatMap(_._2)

        case OffsetKey.StringKey(_) =>
          creates ++ Stream.emit(DataEvent.Commit(OffsetKey.Actual.string("id-100")))

        case OffsetKey.DateTimeKey(_) =>
          val epoch = Instant.EPOCH.atOffset(ZoneOffset.UTC)
          creates ++ Stream.emit(DataEvent.Commit(OffsetKey.Actual.dateTime(epoch)))

        case OffsetKey.ExternalKey(ex) =>
          creates ++ Stream.emit(DataEvent.Commit(OffsetKey.Actual.external(ExternalOffsetKey.empty)))
      }
    }

    def renderAppend[P](
        input: Stream[IO, String],
        columns: PushColumns[Column[ColumnType.Scalar]],
        config: RenderConfig[P],
        limit: Option[Long])
        : Stream[IO, AppendEvent[P, OffsetKey.Actual[ExternalOffsetKey]]] = {
      val creates: Stream[IO, AppendEvent[P, OffsetKey.Actual[ExternalOffsetKey]]] = config match {
        case (r: RenderConfig.Csv) =>
          limit.fold(input)(input.take(_))
            .through(text.utf8Encode)
            .chunks
            .map(DataEvent.Create(_))
        case (r: RenderConfig.Separated) =>
          limit.fold(input)(input.take(_))
            .chunks
            .map(DataEvent.Create(_))
        case _ =>
          Stream.raiseError[IO](new RuntimeException("renderJson not implemented"))
      }
      def commit(i: Int): AppendEvent[P, OffsetKey.Actual[ExternalOffsetKey]] =
        DataEvent.Commit(OffsetKey.Actual.external(ExternalOffsetKey(new Array(i))))

      def go(stream: Stream[IO, AppendEvent[P, OffsetKey.Actual[ExternalOffsetKey]]], acc: Int)
          : Pull[IO, AppendEvent[P, OffsetKey.Actual[ExternalOffsetKey]], Unit] = stream.pull.uncons1 flatMap {
        case None =>
          Pull.output1(commit(acc)) >> Pull.done
        case Some((a@DataEvent.Create(chnk), tail)) =>
          Pull.output1(a) >> go(tail, acc + chnk.size)
        case Some((a, tail)) =>
          Pull.output1(a) >> go(tail, acc)
      }
      go(creates, 0).stream
    }
  }

  def mkEvaluator(f: PartialFunction[(String, Option[Offset]), IO[Stream[IO, String]]])
      : QueryEvaluator[Resource[IO, ?], (String, Option[Offset]), Stream[IO, String]] =
    QueryEvaluator(f).mapF(Resource.liftF(_))

  def constEvaluator(s: IO[Stream[IO, String]])
      : QueryEvaluator[Resource[IO, ?], (String, Option[Offset]), Stream[IO, String]] =
    QueryEvaluator(_ => Resource.liftF(s))

  val emptyEvaluator: QueryEvaluator[Resource[IO, ?], (String, Option[Offset]), Stream[IO, String]] =
    constEvaluator(IO(Stream()))

  trait StreamControl[F[_]] {
    // Cause the stream to emit a value
    def emit(s: String): F[Unit]

    // Forcibly halts the stream
    def halt: F[Unit]
  }

  val controlledStream: IO[(StreamControl[IO], Stream[IO, String])] =
    Queue.unbounded[IO, Option[String]] map { q =>
      val s = q.dequeue.unNoneTerminate

      val ctl = new StreamControl[IO] {
        def emit(v: String) = q.enqueue1(Some(v))
        val halt = q.enqueue1(None)
      }

      (ctl, s)
    }

  val dataStream: Stream[IO, String] =
    Stream.fixedDelay[IO](100.millis).zipRight(Stream(W1, W2, W3))

  def startLatch[A](s: Stream[IO, A], count: Int = 0): IO[(Stream[IO, A], IO[Unit])] =
    Deferred[IO, Unit] map { latch =>
      val latchIdx = count - 1

      val latched =
        if (count <= 0)
          Stream.eval_(latch.complete(())) ++ s
        else
          s.zipWithIndex flatMap {
            case (a, i) if i == latchIdx =>
              Stream.emit(a) ++ Stream.eval_(latch.complete(()))

            case (a, _) =>
              Stream.emit(a)
          }

      (latched, latch.get)
    }

  def mkResultPush(
      destinations: Map[Int, Destination[IO]],
      evaluator: QueryEvaluator[Resource[IO, ?], (String, Option[Offset]), Stream[IO, String]],
      maxConcurrentPushes: Int = 10)
      : Resource[IO, ResultPush[IO, Int, String]] = {

    val lookupDestination: Int => IO[Option[Destination[IO]]] =
      destinationId => IO(destinations.get(destinationId))

    val render = new MockResultRender

    for {
      db <- storage.offheapMVStore[IO]

      pushes <- Resource liftF {
        SerializableStore[IO, Int :: ResourcePath :: HNil, ∃[Push[?, String]]](
          db,
          "default-result-push-spec",
          Blocker.liftExecutionContext(global))
      }

      ref <- Resource.liftF(Ref[IO].of(IMap.empty[Int :: ResourcePath :: HNil, ∃[OffsetKey.Actual]]))

      offsets = RefIndexedStore(ref)

      resultPush <-
        DefaultResultPush[IO, Int, String, Stream[IO, String]](
          maxConcurrentPushes,
          maxConcurrentPushes,
          lookupDestination,
          evaluator,
          render,
          pushes,
          offsets)

    } yield resultPush
  }

  def await[A](io: IO[A]): IO[A] =
    io.timeoutTo(Timeout, IO.raiseError[A](new RuntimeException(s"Expected completion within ${Timeout}.")))

  val outputColumns: Traversal[∃[PushConfig[?, String]], PushConfig.OutputColumn] =
    new Traversal[∃[PushConfig[?, String]], PushConfig.OutputColumn] {
      def modifyF[F[_]: scalaz.Applicative](
          f: PushConfig.OutputColumn => F[PushConfig.OutputColumn])(
          s: ∃[PushConfig[?, String]]) = {

        val c: PushConfig[_, String] = s.value

        c match {
          case x @ PushConfig.Full(p, q, cs) =>
            cs.traverse(f).map(xs => ∃[PushConfig[?, String]](x.copy(columns = xs)))

          case x @ PushConfig.Incremental(_, _, cs, _, _) =>
            cs.traverse(f).map(xs => ∃[PushConfig[?, String]](x.copy(outputColumns = xs)))

          case x @ PushConfig.SourceDriven(_, _, cs) =>
            cs.traverse(f).map(xs => ∃[PushConfig[?, String]](x.copy(outputColumns = xs)))
        }
      }
    }

  val colX = NonEmptyList.one(Column("X", (ColumnType.Boolean, SelectedType(TypeIndex(0), Nil))))

  val resumePos =
    ResumeConfig(
      Column("id", (IdType.StringId, SelectedType(TypeIndex(1), List(∃(Actual.integer(10)))))),
      Column("pos", OffsetKey.Formal.real(())),
      OffsetPath(DataPathSegment.Field("pos")))

  def full(path: ResourcePath, q: String, cols: PushConfig.Columns = colX): ∃[PushConfig[?, String]] =
    ∃[PushConfig[?, String]](PushConfig.Full(path, q, cols))

  def incremental(
      path: ResourcePath,
      q: String,
      cols: List[PushConfig.OutputColumn] = colX.toList,
      resume: ResumeConfig[Real] = resumePos,
      initial: Option[OffsetKey.Actual[Real]] = None)
      : ∃[PushConfig[?, String]] =
    ∃[PushConfig[?, String]](PushConfig.Incremental(path, q, cols, resume, initial))

  def sourceDriven(
      path: ResourcePath,
      q: String,
      cols: PushConfig.Columns = colX)
      : ∃[PushConfig[*, String]] =
    ∃[PushConfig[*, String]](PushConfig.SourceDriven(path, q, PushColumns.NoPrimary(cols)))

  def forallConfigs[A: AsResult](f: ∃[PushConfig[?, String]] => IO[A]): Fragment = {
    "full" >>* f(full(ResourcePath.root() / ResourceName("full"), "fullq"))
    "incremental" >>* f(incremental(ResourcePath.root() / ResourceName("incremental"), "incrementalq"))
    "sourceDriven" >>* f(sourceDriven(ResourcePath.root() / ResourceName("sourceDriven"), "sourceDrivenq"))
  }

  def nonAppendConfigs[A: AsResult](f: ∃[PushConfig[*, String]] => IO[A]): Fragment = {
    "full" >>* f(full(ResourcePath.root() / ResourceName("full"), "fullq"))
    "incremental" >>* f(incremental(ResourcePath.root() / ResourceName("incremental"), "incrementalq"))
  }

  "start" >> {
    "asynchronously pushes results to a destination" >> forallConfigs { config =>
      for {
        (destination, filesystem) <- QDestination()

        r <- mkResultPush(Map(DestinationId -> destination), constEvaluator(IO(dataStream))) use { rp =>
          for {
            started <- rp.start(DestinationId, config, None)

            result <- await(started.sequence)

            filesystemAfterPush <- filesystem
          } yield {
            filesystemAfterPush.keySet must equal(Set(config.value.path))
            filesystemAfterPush(config.value.path) must equal(dataString)
            result must beRight.like {
              case Status.Finished(_, _, _) => ok
            }
          }
        }
      } yield r
    }

    "rejects when full push running for same path" >> forallConfigs { config =>
      val path = config.value.path
      val fullCfg = full(path, "conflictq")

      for {
        (destination, filesystem) <- QDestination()
        (ctl, data0) <- controlledStream
        (data, awaitStart) <- startLatch(data0)

        r <- mkResultPush(Map(DestinationId -> destination), constEvaluator(IO(data))) use { rp =>
          for {
            firstStartStatus <- rp.start(DestinationId, fullCfg, None)

            // ensure first start is running to avoid race conditions
            _ <- ctl.emit(W1)
            _ <- awaitStart

            secondStartStatus <- rp.start(DestinationId, config, None)
          } yield {
            firstStartStatus must beRight
            secondStartStatus must beLeft(NonEmptyList.one(PushAlreadyRunning(DestinationId, path)))
          }
        }
      } yield r
    }

    "rejects when incremental push running for same path" >> forallConfigs { config =>
      val path = config.value.path
      val incCfg = incremental(path, "conflictq")

      for {
        (destination, filesystem) <- QDestination()
        (ctl, data0) <- controlledStream
        (data, awaitStart) <- startLatch(data0)

        r <- mkResultPush(Map(DestinationId -> destination), constEvaluator(IO(data))) use { rp =>
          for {
            firstStartStatus <- rp.start(DestinationId, incCfg, None)

            // ensure first start is running to avoid race conditions
            _ <- ctl.emit(W1)
            _ <- awaitStart

            secondStartStatus <- rp.start(DestinationId, config, None)
          } yield {
            firstStartStatus must beRight
            secondStartStatus must beLeft(NonEmptyList.one(PushAlreadyRunning(DestinationId, path)))
          }
        }
      } yield r
    }

    "allows concurrent push to a path in multiple destinations" >> forallConfigs { config =>
      val dest2 = DestinationId.intValue + 1

      for {
        (destination, _) <- QDestination()

        eval = constEvaluator(controlledStream.map(_._2))

        r <- mkResultPush(Map(DestinationId -> destination, dest2 -> destination), eval) use { rp =>
          for {
            firstStartStatus <- rp.start(DestinationId, config, None)
            secondStartStatus <- rp.start(dest2, config, None)
          } yield {
            firstStartStatus must beRight
            secondStartStatus must beRight
          }
        }
      } yield r
    }

    "fails with 'destination not found' when destination doesn't exist" >> forallConfigs { config =>
      mkResultPush(Map(), emptyEvaluator) use { rp =>
        rp.start(DestinationId, config, None)
          .map(_ must beLeft(NonEmptyList.one(DestinationNotFound(DestinationId))))
      }
    }

    "fails when selected type isn't found" >> forallConfigs { config =>
      val idx = TypeIndex(-1)
      val bad = Column("A", (ColumnType.Boolean, SelectedType(idx, Nil)))
      val cfg = outputColumns.set(bad)(config)

      for {
        (dest, _) <- QDestination()

        startStatus <- mkResultPush(Map(DestinationId -> dest), emptyEvaluator) use { rp =>
          rp.start(DestinationId, cfg, None)
        }
      } yield {
        startStatus must beLeft(NonEmptyList.one(TypeNotFound(DestinationId, "A", idx)))
      }
    }

    "fails when a required type argument is missing" >> forallConfigs { config =>
      val idx = TypeIndex(1)
      val bad = Column("A", (ColumnType.String, SelectedType(idx, Nil)))
      val cfg = outputColumns.set(bad)(config)

      for {
        (dest, _) <- QDestination()

        startStatus <-
          mkResultPush(Map(DestinationId -> dest), emptyEvaluator)
            .use(_.start(DestinationId, cfg, None))
      } yield {
        startStatus must beLeft.like {
          case NonEmptyList(TypeConstructionFailed(
            DestinationId,
            "A",
            "VARCHAR",
            NonEmptyList(ParamError.ParamMissing("Length", _), Nil)), Nil) => ok
        }
      }
    }

    "fails when wrong kind of type argument given" >> forallConfigs { config =>
      val idx = TypeIndex(1)
      val bad = Column("A", (ColumnType.String, SelectedType(idx, List(∃(Actual.boolean(false))))))
      val cfg = outputColumns.set(bad)(config)

      for {
        (dest, _) <- QDestination()

        startStatus <-
          mkResultPush(Map(DestinationId -> dest), emptyEvaluator)
            .use(_.start(DestinationId, cfg, None))
      } yield {
        startStatus must beLeft.like {
          case NonEmptyList(TypeConstructionFailed(
            DestinationId,
            "A",
            "VARCHAR",
            NonEmptyList(ParamError.ParamMismatch("Length", _, _), Nil)), Nil) => ok
        }
      }
    }

    "fails when integer type argument is out of bounds" >> forallConfigs { config =>
      val idx = TypeIndex(1)
      val low = Column("A", (ColumnType.String, SelectedType(idx, List(∃(Actual.integer(0))))))
      val high = Column("A", (ColumnType.String, SelectedType(idx, List(∃(Actual.integer(300))))))

      val cfgL = outputColumns.set(low)(config)
      val cfgH = outputColumns.set(high)(config)

      for {
        (dest, _) <- QDestination()

        r <- mkResultPush(Map(DestinationId -> dest), emptyEvaluator) use { rp =>
          for {
            startLow <- rp.start(DestinationId, cfgL, None)
            startHigh <- rp.start(DestinationId, cfgH, None)
          } yield {
            startLow must beLeft.like {
              case NonEmptyList(TypeConstructionFailed(
                DestinationId,
                "A",
                "VARCHAR",
                NonEmptyList(ParamError.IntOutOfBounds("Length", 0, Ior.Both(1, 256)), Nil)), Nil) => ok
            }

            startHigh must beLeft.like {
              case NonEmptyList(TypeConstructionFailed(
                DestinationId,
                "A",
                "VARCHAR",
                NonEmptyList(ParamError.IntOutOfBounds("Length", 300, Ior.Both(1, 256)), Nil)), Nil) => ok
            }
          }
        }
      } yield r
    }

    "fails when enum type argument selection is not found" >> forallConfigs { config =>
      val idx = TypeIndex(2)
      val bad = Column("B", (ColumnType.Number, SelectedType(idx, List(∃(Actual.enumSelect("32-bits"))))))
      val cfg = outputColumns.set(bad)(config)

      for {
        (dest, _) <- QDestination()

        startStatus <-
          mkResultPush(Map(DestinationId -> dest), emptyEvaluator)
            .use(_.start(DestinationId, cfg, None))
      } yield {
        startStatus must beLeft.like {
          case NonEmptyList(TypeConstructionFailed(
            DestinationId,
            "B",
            "NUMBER",
            NonEmptyList(ParamError.ValueNotInEnum("Width", "32-bits", xs), Nil)), Nil) =>
              xs must_=== NonEmptySet.of("4-bits", "8-bits", "16-bits")
        }
      }
    }

    "fails when column type not a valid coercion of corresponding scalar type" >> forallConfigs { config =>
      val idx = TypeIndex(0) // Boolean
      val bad = Column("B", (ColumnType.Number, SelectedType(idx, Nil)))
      val cfg = outputColumns.set(bad)(config)

      for {
        (dest, _) <- QDestination()

        startStatus <-
          mkResultPush(Map(DestinationId -> dest), emptyEvaluator)
            .use(_.start(DestinationId, cfg, None))
      } yield {
        startStatus must beLeft.like {
          case NonEmptyList(InvalidCoercion(
            DestinationId,
            "B",
            ColumnType.Number,
            idx), Nil) => ok
        }
      }
    }

    "fails when excessive arguments given" >> forallConfigs { config =>
      val idx = TypeIndex(1)
      val bad = Column("A", (ColumnType.String, SelectedType(idx, List(∃(Actual.integer(5)), ∃(Actual.boolean(true))))))
      val cfg = outputColumns.set(bad)(config)

      for {
        (dest, _) <- QDestination()

        startStatus <-
          mkResultPush(Map(DestinationId -> dest), emptyEvaluator)
            .use(_.start(DestinationId, cfg, None))
      } yield {
        startStatus must beLeft.like {
          case NonEmptyList(TypeConstructionFailed(
            DestinationId,
            "A",
            "VARCHAR",
            NonEmptyList(ParamError.ExcessiveParams(1, 2, NonEmptyList(∃(ParamType.Boolean(_)), Nil)), Nil)), Nil) => ok
        }
      }
    }
    "replaces a previous full push to path" >> nonAppendConfigs { config =>
      for {
        (dest, filesystem) <- QDestination()

        prev = full(config.value.path, "prevQ")

        eval = mkEvaluator {
          case ("prevQ", _) => IO(dataStream.take(1))
          case _ => IO(dataStream)
        }

        r <- mkResultPush(Map(DestinationId -> dest), eval) use { rp =>
          for {
            prv <- rp.start(DestinationId, prev, None)
            _ <- await(prv.sequence)
            fs0 <- filesystem

            res <- rp.start(DestinationId, config, None)
            terminal <- await(res.sequence)

            fs1 <- filesystem
          } yield {
            terminal must beRight.like {
              case Status.Finished(_, _, _) => ok
            }
            fs0.get(config.value.path) must beSome(W1)
            fs1.get(config.value.path) must beSome(dataString)
          }
        }
      } yield r
    }

    "replaces a previous incremental push to path" >> nonAppendConfigs { config =>
      for {
        (dest, filesystem) <- QDestination()

        prev = incremental(config.value.path, "prevQ")

        eval = mkEvaluator {
          case ("prevQ", _) => IO(dataStream.take(1))
          case _ => IO(dataStream)
        }

        r <- mkResultPush(Map(DestinationId -> dest), eval) use { rp =>
          for {
            prv <- rp.start(DestinationId, prev, None)
            _ <- await(prv.sequence)
            fs0 <- filesystem

            res <- rp.start(DestinationId, config, None)
            terminal <- await(res.sequence)

            fs1 <- filesystem
          } yield {
            terminal must beRight.like {
              case Status.Finished(_, _, _) => ok
            }
            fs0.get(config.value.path) must beSome(W1)
            fs1.get(config.value.path) must beSome(dataString)
          }
        }
      } yield r
    }

    "replaces a previous source driven push to path" >> nonAppendConfigs { config =>
      for {
        (dest, filesystem) <- QDestination()

        prev = sourceDriven(config.value.path, "prevQ")

        eval = mkEvaluator {
          case ("prevQ", _) => IO(dataStream.take(1))
          case _ => IO(dataStream)
        }

        r <- mkResultPush(Map(DestinationId -> dest), eval) use { rp =>
          for {
            prv <- rp.start(DestinationId, prev, None)
            _ <- await(prv.sequence)
            fs0 <- filesystem

            res <- rp.start(DestinationId, config, None)
            terminal <- await(res.sequence)

            fs1 <- filesystem
          } yield {
            terminal must beRight.like {
              case Status.Finished(_, _, _) => ok
            }
            fs0.get(config.value.path) must beSome(W1)
            fs1.get(config.value.path) must beSome(dataString)
          }
        }
      } yield r
    }
    "appends to previous push result for append configs" >> forallConfigs { prevConfig =>
      for {
        (dest, filesystem) <- QDestination()
        config = sourceDriven(prevConfig.value.path, prevConfig.value.query)
        eval = mkEvaluator {
          case (q, _) if q === prevConfig.value.query => IO(dataStream)
          case _ => IO.never
        }
        r <- mkResultPush(Map(DestinationId -> dest), eval) use { rp =>
          for {
            first <- rp.start(DestinationId, config, None)
            _ <- await(first.sequence)
            fs0 <- filesystem

            res <- rp.start(DestinationId, config, None)
            terminal <- await(res.sequence)
            fs1 <- filesystem
          } yield {
            terminal must beRight.like {
              case Status.Finished(_, _, _) => ok
            }
            fs0.get(config.value.path) must beSome(dataString)
            fs1.get(config.value.path) must beSome(dataString + dataString)
          }
        }
      } yield r
    }

    "limit restricts the number of rows pushed" >> forallConfigs { config =>
      for {
        (destination, filesystem) <- QDestination()

        r <- mkResultPush(Map(DestinationId -> destination), constEvaluator(IO(dataStream))) use { rp =>
          for {
            started <- rp.start(DestinationId, config, Some(2L))

            result <- await(started.sequence)

            filesystemAfterPush <- filesystem
          } yield {
            filesystemAfterPush.keySet must equal(Set(config.value.path))
            filesystemAfterPush(config.value.path) must equal(W1+W2)
            result must beRight.like {
              case Status.Finished(_, _, _) => ok
            }
          }
        }
      } yield r
    }

    "full fails when destination doesn't support full push" >>* {
      for {
        (dest, _) <- QDestination(supports = QDestination.Upsert)

        startStatus <-
          mkResultPush(Map(DestinationId -> dest), emptyEvaluator)
            .use(_.start(DestinationId, full(ResourcePath.root(), "nope"), None))
      } yield {
        startStatus must beLeft.like {
          case NonEmptyList(FullNotSupported(DestinationId), Nil) => ok
        }
      }
    }

    "incremental fails when destination doesn't support incremental push" >>* {
      for {
        (dest, _) <- QDestination(supports = QDestination.Create)

        startStatus <-
          mkResultPush(Map(DestinationId -> dest), emptyEvaluator)
            .use(_.start(DestinationId, incremental(ResourcePath.root(), "nope"), None))
      } yield {
        startStatus must beLeft.like {
          case NonEmptyList(IncrementalNotSupported(DestinationId), Nil) => ok
        }
      }
    }

    "starts incremental push from initial offset" >>* {
      for {
        (destination, filesystem) <- QDestination()

        incCfg = incremental(
          path = ResourcePath.root() / ResourceName("initial"),
          q = "initial-offset",
          initial = Some(OffsetKey.Actual.real(99)))

        eval = mkEvaluator {
          case ("initial-offset", Some(Offset.Internal(NonEmptyList(DataPathSegment.Field("pos"), Nil), ∃(k)))) =>
            val key: OffsetKey.Actual[_] = k

            key match {
              case OffsetKey.RealKey(r) if r.toInt == 99 => IO(dataStream)
              case _ => IO.never
            }
        }

        r <- mkResultPush(Map(DestinationId -> destination), eval) use { rp =>
          for {
            started <- rp.start(DestinationId, incCfg, None)

            result <- await(started.sequence)

            filesystemAfterPush <- filesystem
          } yield {
            filesystemAfterPush.keySet must equal(Set(incCfg.value.path))
            filesystemAfterPush(incCfg.value.path) must equal(dataString)
            result must beRight.like {
              case Status.Finished(_, _, _) => ok
            }
          }
        }
      } yield r
    }
  }

  "update" >> {
    "restarts previous full push using saved query" >>* {
      for {
        (dest, filesystem) <- QDestination()

        (ctl, data) <- controlledStream

        path = ResourcePath.root() / ResourceName("updatefull")
        config = full(path, "updatefull")

        eval = mkEvaluator {
          case ("updatefull", None) => IO(data)
        }

        r <- mkResultPush(Map(DestinationId -> dest), eval) use { rp =>
          for {
            started <- rp.start(DestinationId, config, None)

            _ <- ctl.emit(W1)
            _ <- ctl.emit(W2)
            _ <- ctl.halt

            startRes <- await(started.sequence)
            fs1 <- filesystem

            updated <- rp.update(DestinationId, config.value.path)

            _ <- ctl.emit(W1)
            _ <- ctl.emit(W2)
            _ <- ctl.emit(W2)
            _ <- ctl.emit(W3)
            _ <- ctl.halt

            updateRes <- await(updated.sequence)
            fs2 <- filesystem
          } yield {
            startRes must beRight.like {
              case Status.Finished(_, _, _) => ok
            }
            fs1.get(path) must beSome(W1+W2)

            updateRes must beRight.like {
              case Status.Finished(_, _, _) => ok
            }
            fs2.get(path) must beSome(W1+W2+W2+W3)
          }
        }
      } yield r
    }

    "resumes previous incremental push from saved offset" >>* {
      for {
        (dest, filesystem) <- QDestination()

        path = ResourcePath.root() / ResourceName("resume")
        config = incremental(path, "resume")

        eval = mkEvaluator {
          case ("resume", None) =>
            IO(Stream(W1, W1, W1).covary[IO])

          case ("resume", Some(Offset.Internal(_, ∃(k)))) =>
            (k: OffsetKey.Actual[_]) match {
              case OffsetKey.RealKey(r) if r.toInt == 15 =>
                IO(Stream(W2, W2, W2).covary[IO])
              case _ => IO.never
            }
        }

        r <- mkResultPush(Map(DestinationId -> dest), eval) use { rp =>
          for {
            started <- rp.start(DestinationId, config, None)

            // offset = 15 since 3 emits
            startRes <- await(started.sequence)
            fs1 <- filesystem

            // resume from offset = 15
            updated <- rp.update(DestinationId, config.value.path)

            updateRes <- await(updated.sequence)
            fs2 <- filesystem
          } yield {
            startRes must beRight.like {
              case Status.Finished(_, _, _) => ok
            }
            fs1.get(path) must beSome(W1+W1+W1)

            updateRes must beRight.like {
              case Status.Finished(_, _, _) => ok
            }
            fs2.get(path) must beSome(W1+W1+W1+W2+W2+W2)
          }
        }
      } yield r
    }

    "resumes previous incremental push from beginning when no offset" >>* {
      for {
        (dest, filesystem) <- QDestination()

        (ctl, data) <- controlledStream

        failOn1 = data flatMap {
          case W1 => Stream.raiseError[IO](new RuntimeException("W1!"))
          case s => Stream.emit(s)
        }

        path = ResourcePath.root() / ResourceName("qrs")
        config = incremental(path, "qrs")

        eval = mkEvaluator {
          case ("qrs", None) => IO(failOn1)
          case ("qrs", Some(_)) => IO(dataStream)
        }

        r <- mkResultPush(Map(DestinationId -> dest), eval) use { rp =>
          for {
            started <- rp.start(DestinationId, config, None)

            _ <- ctl.emit(W1) // boom

            // no offset since nothing emitted
            startRes <- await(started.sequence)
            fs1 <- filesystem

            // resume from beginning
            update1 <- rp.update(DestinationId, config.value.path)

            _ <- ctl.emit(W2)
            _ <- ctl.emit(W1) // boom

            // offset = 5 since emitted a value before boom
            update1Res <- await(update1.sequence)
            fs2 <- filesystem

            // resume from offset = 5
            update2 <- rp.update(DestinationId, config.value.path)

            update2Res <- await(update2.sequence)
            fs3 <- filesystem
          } yield {
            startRes must beRight.like {
              case Status.Failed(_, _, _, _) => ok
            }
            fs1.get(path) must beSome("")

            update1Res must beRight.like {
              case Status.Failed(_, _, _, _) => ok
            }
            fs2.get(path) must beSome(W2)

            update2Res must beRight.like {
              case Status.Finished(_, _, _) => ok
            }
            fs3.get(path) must beSome(W2+dataString)
          }
        }
      } yield r
    }

    "resumes previous incremental that failed prior to commit from initial" >>* {
      for {
        (dest, filesystem) <- QDestination()

        (ctl, data) <- controlledStream

        failOn2 = data flatMap {
          case W2 => Stream.raiseError[IO](new RuntimeException("W2!"))
          case s => Stream.emit(s)
        }

        path = ResourcePath.root() / ResourceName("abc")
        // initial offset = 17
        config = incremental(path, "abc", initial = Some(OffsetKey.Actual.real(17)))

        // only emits for offset = 17
        eval = mkEvaluator {
          case ("abc", Some(Offset.Internal(_, ∃(k)))) =>
            (k: OffsetKey.Actual[_]) match {
              case OffsetKey.RealKey(r) if r.toInt == 17 => IO(failOn2)
              case _ => IO.never
            }
        }

        r <- mkResultPush(Map(DestinationId -> dest), eval) use { rp =>
          for {
            started <- rp.start(DestinationId, config, None)

            _ <- ctl.emit(W2) // boom

            // offset still = 17 since nothing emitted
            startRes <- await(started.sequence)
            fs1 <- filesystem

            // resume from 17
            update1 <- rp.update(DestinationId, config.value.path)

            _ <- ctl.emit(W1)
            _ <- ctl.emit(W3)
            _ <- ctl.halt

            // offset = 10 since emitted two values before halt
            update1Res <- await(update1.sequence)
            fs2 <- filesystem
          } yield {
            startRes must beRight.like {
              case Status.Failed(_, _, _, _) => ok
            }
            fs1.get(path) must beSome("")

            update1Res must beRight.like {
              case Status.Finished(_, _, _) => ok
            }
            fs2.get(path) must beSome(W1+W3)
          }
        }
      } yield r
    }

    "resumes append push from saved offset" >>* {
      for {
        (dest, filesystem) <- QDestination()

        path = ResourcePath.root() / ResourceName("qrs")
        config = sourceDriven(path, "append")

        eval = mkEvaluator {
          case ("append", None) =>
            IO(Stream(W1, W1, W1).covary[IO])

          case ("append", Some(Offset.External(∃(k)))) =>
            (k: OffsetKey.Actual[_]) match {
              case OffsetKey.ExternalKey(ek) if ek.value.length === 15 =>
                IO(Stream(W2, W2, W2).covary[IO])
              case _ => IO.never
            }
        }

        r <- mkResultPush(Map(DestinationId -> dest), eval) use { rp =>
          for {
            started <- rp.start(DestinationId, config, None)

            startRes <- await(started.sequence)
            fs1 <- filesystem

            updated <- rp.update(DestinationId, config.value.path)

            updateRes <- await(updated.sequence)
            fs2 <- filesystem
            } yield {
              startRes must beRight.like {
                case Status.Finished(_, _, _) => ok
              }
              fs1.get(path) must beSome(W1+W1+W1)

              updateRes must beRight.like {
                case Status.Finished(_, _, _) => ok
              }
              fs2.get(path) must beSome(W1+W1+W1+W2+W2+W2)
            }
        }
      } yield r
    }
    "retains committed incremental offset on failure" >>* {
      for {
        (dest, filesystem) <- QDestination()

        (ctl, data) <- controlledStream

        fail3rd = data.zipWithIndex flatMap {
          case (_, 2) => Stream.raiseError[IO](new RuntimeException("3RD!"))
          case (s, _) => Stream.emit(s)
        }

        path = ResourcePath.root() / ResourceName("xyz")
        config = incremental(path, "xyz")

        eval = mkEvaluator {
          case ("xyz", None) => IO(data)
          case ("xyz", Some(Offset.Internal(_, ∃(k)))) =>
            (k: OffsetKey.Actual[_]) match {
              case OffsetKey.RealKey(r) if r.toInt == 5 => IO(fail3rd)
              case OffsetKey.RealKey(r) if r.toInt == 10 => IO(data)
              case _ => IO.never
            }
        }

        r <- mkResultPush(Map(DestinationId -> dest), eval) use { rp =>
          for {
            started <- rp.start(DestinationId, config, None)

            _ <- ctl.emit(W1)
            _ <- ctl.halt

            // offset = 5
            startRes <- await(started.sequence)
            fs1 <- filesystem

            // resume from 5
            update1 <- rp.update(DestinationId, config.value.path)

            _ <- ctl.emit(W2)
            _ <- ctl.emit(W2) // offset = 10
            _ <- ctl.emit(W2) // boom!

            // offset = 10 since emitted two values before boom
            update1Res <- await(update1.sequence)
            fs2 <- filesystem

            // resume from 10
            update2 <- rp.update(DestinationId, config.value.path)

            _ <- ctl.emit(W3)
            _ <- ctl.emit(W3)
            _ <- ctl.emit(W3)
            _ <- ctl.halt

            // offset = 15
            update2Res <- await(update2.sequence)
            fs3 <- filesystem
          } yield {
            startRes must beRight.like {
              case Status.Finished(_, _, _) => ok
            }
            fs1.get(path) must beSome(W1)

            update1Res must beRight.like {
              case Status.Failed(_, _, _, _) => ok
            }
            fs2.get(path) must beSome(W1+W2+W2)

            update2Res must beRight.like {
              case Status.Finished(_, _, _) => ok
            }
            fs3.get(path) must beSome(W1+W2+W2+W3+W3+W3)
          }
        }
      } yield r
    }

    "fails if never started" >>* {
      for {
        (dest, _) <- QDestination()

        path = ResourcePath.root() / ResourceName("nope")

        updated <-
          mkResultPush(Map(DestinationId -> dest), emptyEvaluator)
            .use(_.update(DestinationId, path))
      } yield {
        updated must beLeft.like {
          case NonEmptyList(PushNotFound(DestinationId, p), Nil) => p must equal(path)
        }
      }
    }

    "fails if already running" >> forallConfigs { config =>
      for {
        (dest, filesystem) <- QDestination()

        latch <- Deferred[IO, Unit]

        (ctl, data0) <- controlledStream

        data = data0 flatMap {
          case W2 => Stream.eval(latch.complete(())).as(W2)
          case other => Stream.emit(other)
        }

        r <- mkResultPush(Map(DestinationId -> dest), constEvaluator(IO(data))) use { rp =>
          for {
            started <- rp.start(DestinationId, config, None)

            _ <- ctl.emit(W1)
            _ <- ctl.halt

            startRes <- await(started.sequence)

            update1 <- rp.update(DestinationId, config.value.path)

            // ensure update1 is running to avoid race conditions
            _ <- ctl.emit(W2)
            _ <- latch.get

            update2 <- rp.update(DestinationId, config.value.path)
          } yield {
            startRes must beRight.like {
              case Status.Finished(_, _, _) => ok
            }
            update1 must beRight
            update2 must beLeft.like {
              case NonEmptyList(PushAlreadyRunning(DestinationId, p), Nil) =>
                p must equal(config.value.path)
            }
          }
        }
      } yield r
    }
  }

  "cancel" >> {
    "halts a running initial push" >> forallConfigs { config =>
      val path = config.value.path

      for {
        (destination, filesystem) <- QDestination()

        (ctl, data0) <- controlledStream
        (data, awaitStart) <- startLatch(data0, 1)

        r <- mkResultPush(Map(DestinationId -> destination), constEvaluator(IO(data))) use { rp =>
          for {
            startRes <- rp.start(DestinationId, config, None)

            _ <- ctl.emit(W1)
            _ <- awaitStart

            filesystemAfterPush <- filesystem

            cancelRes <- rp.cancel(DestinationId, path)

            terminal <- await(startRes.sequence)
          } yield {
            filesystemAfterPush.keySet must equal(Set(path))
            // check if a *partial* result was pushed
            filesystemAfterPush(path) must equal(W1)
            cancelRes must beNormal
            terminal must beRight.like {
              case Status.Canceled(_, _, _) => ok
            }
          }
        }
      } yield r
    }

    "no-op when push already completed" >> forallConfigs { config =>
      for {
        (destination, _) <- QDestination()

        r <- mkResultPush(Map(DestinationId -> destination), constEvaluator(IO(dataStream))) use { rp =>
          for {
            startRes <- rp.start(DestinationId, config, None)

            _ <- await(startRes.sequence)

            cancelRes <- rp.cancel(DestinationId, config.value.path)
          } yield cancelRes must beNormal
        }
      } yield r
    }

    "no-op when no push to path" >>* {
      for {
        (destination, _) <- QDestination()

        cancelRes <-
          mkResultPush(Map(DestinationId -> destination), emptyEvaluator)
            .use(_.cancel(DestinationId, ResourcePath.root() / ResourceName("out")))
      } yield {
        cancelRes must beNormal
      }
    }

    "returns 'destination not found' when destination doesn't exist" >>* {
      mkResultPush(Map(), emptyEvaluator)
        .use(_.cancel(DestinationId, ResourcePath.root()))
        .map(_ must beAbnormal(DestinationNotFound(DestinationId)))
    }
  }

  "pushedTo" >> {
    def lookupPushed(
        r: Either[DestinationNotFound[Int], Map[ResourcePath, ∃[Push[?, String]]]],
        p: ResourcePath)
        : Option[Push[_, String]] =
      r.toOption.flatMap(_.get(p)).map(_.value)

    "includes running push" >> forallConfigs { config =>
      for {
        (destination, _) <- QDestination()

        (ctl, data0) <- controlledStream
        (data, awaitStart) <- startLatch(data0)

        pushed <- mkResultPush(Map(DestinationId -> destination), constEvaluator(IO(data))) use { rp =>
          for {
            _ <- rp.start(DestinationId, config, None)

            _ <- ctl.emit(W1)
            _ <- awaitStart

            ps <- rp.pushedTo(DestinationId)
          } yield ps
        }
      } yield {
        lookupPushed(pushed, config.value.path) must beSome.like {
          case Push(c, _, Status.Running(_, _)) =>
            c.path must equal(config.value.path)
            c.query must equal(config.value.query)
        }
      }
    }

    "includes running push when previous terminal exists" >> forallConfigs { config =>
      for {
        (destination, _) <- QDestination()

        cfg0 = config.value
        cfgQ = cfg0.query
        initialQ = cfgQ + "-initial"

        initialCfg = ∃[PushConfig[?, String]](PushConfig.query.set(initialQ)(cfg0))

        (ctl, data0) <- controlledStream
        (data, awaitStart) <- startLatch(data0)

        eval = mkEvaluator {
          case (`initialQ`, None) => IO(dataStream)
          case (`cfgQ`, None) => IO(data)
        }

        r <- mkResultPush(Map(DestinationId -> destination), eval) use { rp =>
          for {
            initRes <- rp.start(DestinationId, initialCfg, None)
            _ <- await(initRes.sequence)

            initPushed <- rp.pushedTo(DestinationId)

            _ <- rp.start(DestinationId, config, None)

            _ <- ctl.emit(W1)
            _ <- awaitStart

            nextPushed <- rp.pushedTo(DestinationId)
          } yield {
            lookupPushed(initPushed, config.value.path) must beSome.like {
              case Push(c, _, Status.Finished(_, _, _)) =>
                c.query must_=== initialQ
            }

            lookupPushed(nextPushed, config.value.path) must beSome.like {
              case Push(c, _, Status.Running(_, _)) =>
                c.query must_=== cfgQ
            }
          }
        }
      } yield r
    }

    "includes canceled push" >> forallConfigs { config =>
      for {
        (destination, _) <- QDestination()

        (ctl, data0) <- controlledStream
        (data, awaitStarted) <- startLatch(data0)

        pushed <- mkResultPush(Map(DestinationId -> destination), constEvaluator(IO(data))) use { rp =>
          for {
            startRes <- rp.start(DestinationId, config, None)

            _ <- ctl.emit(W1)
            _ <- awaitStarted

            _ <- rp.cancel(DestinationId, config.value.path)

            _ <- await(startRes.sequence)

            ps <- rp.pushedTo(DestinationId)
          } yield ps
        }
      } yield {
        lookupPushed(pushed, config.value.path) must beSome.like {
          case Push(c, _, Status.Canceled(_, _, _)) =>
            c.path must equal(config.value.path)
            c.query must equal(config.value.query)
        }
      }
    }

    "includes completed push" >> forallConfigs { config =>
      for {
        (destination, _) <- QDestination()

        pushed <- mkResultPush(Map(DestinationId -> destination), constEvaluator(IO(dataStream))) use { rp =>
          for {
            startRes <- rp.start(DestinationId, config, None)
            _ <- await(startRes.sequence)

            ps <- rp.pushedTo(DestinationId)
          } yield ps
        }
      } yield {
        lookupPushed(pushed, config.value.path) must beSome.like {
          case Push(c, _, Status.Finished(_, _, _)) =>
            c.path must equal(config.value.path)
            c.query must equal(config.value.query)
        }
      }
    }

    "includes pushes that failed to initialize along with error that led to failure" >> forallConfigs { config =>
      for {
        (destination, _) <- QDestination()

        eval = constEvaluator(IO.raiseError[Stream[IO, String]](new Exception("boom")))

        pushed <- mkResultPush(Map(DestinationId -> destination), eval) use { rp =>
          for {
            startRes <- rp.start(DestinationId, config, None)
            _ <- await(startRes.sequence)
            ps <- rp.pushedTo(DestinationId)
          } yield ps
        }
      } yield {
        lookupPushed(pushed, config.value.path) must beSome.like {
          case Push(c, _, Status.Failed(_, _, _, msg)) =>
            c.path must equal(config.value.path)
            c.query must equal(config.value.query)
            msg must contain("boom")
        }
      }
    }

    "includes pushes that failed during streaming along with the error that led to falure" >> forallConfigs { config =>
      val ex = new Exception("errored!")

      for {
        (destination, _) <- QDestination()

        eval = constEvaluator(IO(dataStream ++ Stream.raiseError[IO](ex)))

        pushed <- mkResultPush(Map(DestinationId -> destination), eval) use { rp =>
          for {
            startRes <- rp.start(DestinationId, config, None)
            _ <- await(startRes.sequence)
            ps <- rp.pushedTo(DestinationId)
          } yield ps
        }
      } yield {
        lookupPushed(pushed, config.value.path) must beSome.like {
          case Push(c, _, Status.Failed(_, _, _, msg)) =>
            c.path must equal(config.value.path)
            c.query must equal(config.value.query)
            msg must contain("errored!")
        }
      }
    }

    "results in previous terminal status if canceled before started" >> forallConfigs { config =>
      for {
        (destination, _) <- QDestination()

        cfg0 = config.value
        origQ = cfg0.query

        initialQ = origQ + "-initial"
        againQ = origQ + "-again"
        otherQ = origQ + "-other"

        initialCfg = ∃[PushConfig[?, String]](PushConfig.query.set(initialQ)(cfg0))
        againCfg = ∃[PushConfig[?, String]](PushConfig.query.set(againQ)(cfg0))

        otherCfg = ∃[PushConfig[?, String]](
          PushConfig.path.modify(_ / ResourceName("other"))(
            PushConfig.query.set(otherQ)(cfg0)))

        (ctl1, data1) <- controlledStream
        (ctl2, data20) <- controlledStream
        (data2, started2) <- startLatch(data20)

        eval = mkEvaluator {
          case (`initialQ`, None) => IO(dataStream)
          case (`againQ`, None) => IO(data1)
          case (`otherQ`, None) => IO(data2)
        }

        r <- mkResultPush(Map(DestinationId -> destination), eval, 1) use { rp =>
          for {
            init <- rp.start(DestinationId, initialCfg, None)
            initRes <- await(init.sequence)

            initPushes <- rp.pushedTo(DestinationId)

            // simulate another push running, blocks 'again' due to concurrency limit
            _ <- rp.start(DestinationId, otherCfg, None)
            again <- rp.start(DestinationId, againCfg, None)

            _ <- ctl2.emit(W1)
            _ <- started2

            otherPushes <- rp.pushedTo(DestinationId)

            _ <- rp.cancel(DestinationId, againCfg.value.path)
            _ <- await(again.sequence)

            canceledPushes <- rp.pushedTo(DestinationId)
          } yield {
            lookupPushed(initPushes, initialCfg.value.path) must beSome.like {
              case Push(c, _, Status.Finished(_, _, _)) => c.query must_=== initialQ
            }

            lookupPushed(otherPushes, otherCfg.value.path) must beSome.like {
              case Push(c, _, Status.Running(_, _)) => c.query must_=== otherQ
            }

            lookupPushed(otherPushes, againCfg.value.path) must beSome.like {
              case Push(c, _, Status.Accepted(_, _)) => c.query must_=== againQ
            }

            lookupPushed(canceledPushes, againCfg.value.path) must beSome.like {
              case Push(c, _, Status.Finished(_, _, _)) => c.query must_=== initialQ
            }
          }
        }
      } yield r
    }

    "empty when nothing has been pushed" >>* {
      for {
        (destination, _) <- QDestination()

        pushes <-
          mkResultPush(Map(DestinationId -> destination), emptyEvaluator)
            .use(_.pushedTo(DestinationId))
      } yield {
        pushes must beLike {
          case Right(m) => m.isEmpty must beTrue
        }
      }
    }

    "returns 'destination not found' when destination doesn't exist" >>* {
      mkResultPush(Map(), emptyEvaluator)
        .use(_.pushedTo(DestinationId))
        .map(_ must beLeft(DestinationNotFound(DestinationId)))
    }
  }

  "cancel all" >> {
    "aborts all running pushes" >>* {
      val path1 = ResourcePath.root() / ResourceName("foo")
      val fullCfg = full(path1, "queryFoo")

      val path2 = ResourcePath.root() / ResourceName("bar")
      val incCfg = incremental(path2, "queryBar")

      for {
        (destination, _) <- QDestination()

        (ctlA, dataA0) <- controlledStream
        (dataA, startedA) <- startLatch(dataA0)
        (ctlB, dataB0) <- controlledStream
        (dataB, startedB) <- startLatch(dataB0)

        eval = mkEvaluator {
          case ("queryFoo", _) => IO(dataA)
          case ("queryBar", _) => IO(dataB)
        }

        r <- mkResultPush(Map(DestinationId -> destination), eval) use { rp =>
          for {
            s1 <- rp.start(DestinationId, fullCfg, None)
            s2 <- rp.start(DestinationId, incCfg, None)

            _ <- ctlA.emit(W1)
            _ <- ctlB.emit(W2)

            _ <- startedA *> startedB

            _ <- rp.cancelAll

            r1 <- await(s1.sequence)
            r2 <- await(s2.sequence)
          } yield {
            r1 must beRight.like {
              case Status.Canceled(_, _, _) => ok
            }

            r2 must beRight.like {
              case Status.Canceled(_, _, _) => ok
            }
          }
        }
      } yield r
    }

    "no-op when no running pushes" >>* {
      mkResultPush(Map(), emptyEvaluator).use(_.cancelAll).as(ok)
    }
  }
}

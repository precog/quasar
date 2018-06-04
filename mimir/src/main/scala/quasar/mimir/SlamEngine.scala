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

import slamdata.Predef._

import quasar._
import quasar.blueeyes.json.JValue
import quasar.common._
import quasar.connector._
import quasar.contrib.cats.effect._
import quasar.contrib.pathy._
import quasar.contrib.scalaz._, eitherT._
import quasar.fp._
import quasar.contrib.iota._
import quasar.fp.numeric._
import quasar.fs._
import quasar.fs.mount._
import quasar.mimir.MimirCake._
import quasar.precog.common.Path
import quasar.qscript._
import quasar.qscript.analysis._
import quasar.qscript.rewrites.{Optimize, Unicoalesce, Unirewrite}
import quasar.yggdrasil.bytecode.JType

import argonaut._, Argonaut._

import cats.effect.IO
import io.chrisdavenport.scalaz.task._

import fs2.{async, Stream}
import fs2.async.mutable.{Queue, Signal}
import fs2.interop.cats.asyncInstance
import fs2.interop.scalaz.{asyncInstance => scalazAsyncInstance}

import matryoshka._
import matryoshka.implicits._
import matryoshka.data._

import org.slf4s.Logging

import pathy.Path._

import scalaz._, Scalaz._
import scalaz.concurrent.Task
import iotaz.CopK

import scala.Predef.implicitly
import scala.concurrent.ExecutionContext.Implicits.global

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

trait SlamEngine extends BackendModule with Logging with DefaultAnalyzeModule {
  import FileSystemError._
  import PathError._
  import Precog.startTask

  val lwc: LightweightConnector

  type QS[T[_[_]]] = MimirQScriptCP[T]

  implicit def qScriptToQScriptTotal[T[_[_]]]: Injectable[QSM[T, ?], QScriptTotal[T, ?]] =
    mimir.qScriptToQScriptTotal[T]

  implicit def qScriptCoreToQScript[T[_[_]]]: Injectable[QScriptCore[T, ?], QSM[T, ?]] =
    Injectable.inject[QScriptCore[T, ?], QSM[T, ?]]

  implicit def equiJoinToQScript[T[_[_]]]: Injectable[EquiJoin[T, ?], QSM[T, ?]] =
    Injectable.inject[EquiJoin[T, ?], QSM[T, ?]]

  implicit def shiftedReadToQScript[T[_[_]]]: Injectable[Const[ShiftedRead[AFile], ?], QSM[T, ?]] =
    Injectable.inject[Const[ShiftedRead[AFile], ?], QSM[T, ?]]

  type Repr = MimirRepr
  type MT[F[_], A] = CakeMT[F, A]
  type M[A] = CakeM[A]

  import Cost._
  import Cardinality._

  def CardinalityQSM: Cardinality[QSM[Fix, ?]] = Cardinality[QSM[Fix, ?]]
  def CostQSM: Cost[QSM[Fix, ?]] = Cost[QSM[Fix, ?]]
  def FunctorQSM[T[_[_]]] = Functor[QSM[T, ?]]
  def TraverseQSM[T[_[_]]] = Traverse[QSM[T, ?]]
  def DelayRenderTreeQSM[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = implicitly[Delay[RenderTree, QSM[T, ?]]]
  def ExtractPathQSM[T[_[_]]: RecursiveT] = ExtractPath[QSM[T, ?], APath]
  def QSCoreInject[T[_[_]]] = implicitly[QScriptCore[T, ?] :<<: QSM[T, ?]]
  def MonadM = Monad[M]
  def UnirewriteT[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = implicitly[Unirewrite[T, QS[T]]]
  def UnicoalesceCap[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = Unicoalesce.Capture[T, QS[T]]

  // the data directory for mimir and the connection uri used to connect to the lightweight connector
  final case class Config(dataDir: java.io.File, uri: ConnectionUri)

  def optimize[T[_[_]]: BirecursiveT: EqualT: ShowT]
      : QSM[T, T[QSM[T, ?]]] => QSM[T, T[QSM[T, ?]]] = {
    val O = new Optimize[T]
    O.optimize(reflNT[QSM[T, ?]])
  }

  // for lightweight connectors that require a connection uri
  // { "dataDir": "/path/to/data/", "uri": "<uri_to_lwc>" }
  //
  // for lightweight connectors that don't require a connection uri (defaults to "")
  // "/path/to/data/"
  def parseConfig(uri: ConnectionUri): BackendDef.DefErrT[Task, Config] = {

    case class Config0(dataDir: String, uri: String)
    implicit val CodecConfig0 = casecodec2(Config0.apply, Config0.unapply)("dataDir", "uri")

    val config0: BackendDef.DefErrT[Task, Config0] = EitherT.eitherT {
      Task.delay {
        uri.value.parse.map(_.as[Config0]) match {
          case Left(err) =>
            Config0(uri.value, "").right[BackendDef.DefinitionError]
          case Right(DecodeResult(Left((err, _)))) =>
            NonEmptyList(err).left[EnvironmentError].left[Config0]
          case Right(DecodeResult(Right(config))) =>
            config.right[BackendDef.DefinitionError]
        }
      }
    }

    config0.flatMap {
      case Config0(dataDir, uri) =>
        val file = new java.io.File(dataDir.value)

        if (!file.isAbsolute)
          EitherT.leftT(NonEmptyList("Mimir cannot be mounted to a relative path").left.point[Task])
        else
          Config(file, ConnectionUri(uri)).point[BackendDef.DefErrT[Task, ?]]
    }
  }

  def compile(cfg: Config): BackendDef.DefErrT[Task, (M ~> Task, Task[Unit])] = {
    val t: Task[String \/ (M ~> Task, Task[Unit])] = for {
      cake <- Precog(cfg.dataDir)
      connector <- lwc.init(cfg.uri).run
    } yield {
      connector.map {
        case (fs, shutdown) =>
          (λ[M ~> Task](_.run((cake: Cake, fs))), cake.shutdown.to[Task] >> shutdown)
      }
    }

    EitherT.eitherT(t).leftMap(err => NonEmptyList(err).left[EnvironmentError])
  }

  // M = Backend
  // F[_] = MapFuncCore[T, ?]
  // B = Repr
  // A = SrcHole
  // AlgebraM[M, CoEnv[A, F, ?], B] = AlgebraM[Backend, CoEnv[Hole, MapFuncCore[T, ?], ?], Repr]
  // def interpretM[M[_], F[_], A, B](f: A => M[B], φ: AlgebraM[M, F, B]): AlgebraM[M, CoEnv[A, F, ?], B]
  // f.cataM(interpretM)
  def plan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](
      cp: T[QSM[T, ?]]): Backend[Repr] = {

    def mapFuncPlanner[F[_]: Monad] = MapFuncPlanner[T, F, MapFunc[T, ?]]

    def qScriptCorePlanner = new QScriptCorePlanner[T, Backend](
      λ[IO ~> Backend](_.to[Task].liftM[MT].liftB),
      λ[M ~> Backend](_.liftB))

    def equiJoinPlanner = new EquiJoinPlanner[T, Backend](
      λ[IO ~> Backend](_.to[Task].liftM[MT].liftB))

    val liftErr: FileSystemErrT[M, ?] ~> Backend =
      Hoist[FileSystemErrT].hoist[M, PhaseResultT[Configured, ?]](
        λ[Configured ~> PhaseResultT[Configured, ?]](_.liftM[PhaseResultT])
          compose λ[M ~> Configured](_.liftM[ConfiguredT]))

    def shiftedReadPlanner = new ShiftedReadPlanner[T, Backend](liftErr)

    lazy val planQST: AlgebraM[Backend, QScriptTotal[T, ?], Repr] = {
      val QScriptCore = CopK.Inject[QScriptCore[T, ?],            QScriptTotal[T, ?]]
      val EquiJoin    = CopK.Inject[EquiJoin[T, ?],               QScriptTotal[T, ?]]
      val ShiftedRead = CopK.Inject[Const[ShiftedRead[AFile], ?], QScriptTotal[T, ?]]
      _ match {
        case QScriptCore(value) => qScriptCorePlanner.plan(planQST)(value)
        case EquiJoin(value)    => equiJoinPlanner.plan(planQST)(value)
        case ShiftedRead(value) => shiftedReadPlanner.plan(value)
        case _ => ???
      }
    }

    def planQSM(in: QSM[T, Repr]): Backend[Repr] = {
      val QScriptCore = CopK.Inject[QScriptCore[T, ?],            QSM[T, ?]]
      val EquiJoin    = CopK.Inject[EquiJoin[T, ?],               QSM[T, ?]]
      val ShiftedRead = CopK.Inject[Const[ShiftedRead[AFile], ?], QSM[T, ?]]
      in match {
        case QScriptCore(value) => qScriptCorePlanner.plan(planQST)(value)
        case EquiJoin(value)    => equiJoinPlanner.plan(planQST)(value)
        case ShiftedRead(value) => shiftedReadPlanner.plan(value)
      }
    }

    cp.cataM[Backend, Repr](planQSM _)
  }

  private def fileToPath(file: AFile): Path = Path(pathy.Path.posixCodec.printPath(file))

  object QueryFileModule extends QueryFileModule {
    import QueryFile._

    private val map = new ConcurrentHashMap[ResultHandle, Precog#TablePager]
    private val cur = new AtomicLong(0L)

    def executePlan(repr: Repr, out: AFile): Backend[Unit] = {
      val path = fileToPath(out)

      // TODO it's kind of ugly that we have to page through JValue to get back into NIHDB
      val driver = for {
        q <- async.boundedQueue[IO, Vector[JValue]](1)

        populator = scala.Predef.locally {
          import shims._
          repr.table.slices.foreachRec { slice =>
            if (!slice.isEmpty) {
              val json = slice.toJsonElements
              if (!json.isEmpty)
                q.enqueue1(json)
              else
                IO.unit
            } else {
              IO.unit
            }
          }
        }

        populatorWithTermination = populator.flatMap(_ => q.enqueue1(Vector.empty))

        ingestor = repr.P.ingest(path, q.dequeue.takeWhile(_.nonEmpty).flatMap(Stream.emits)).run

        // generally this function is bad news (TODO provide a way to ingest as a Stream)
        _ <- Task.gatherUnordered(Seq(populatorWithTermination.to[Task], ingestor.to[Task])).to[IO]
      } yield ()

      driver.to[Task].liftM[MT].liftB
    }

    def evaluatePlan(repr: Repr): Backend[ResultHandle] = {
      val t = for {
        handle <- IO(ResultHandle(cur.getAndIncrement()))
        pager <- repr.P.TablePager(repr.table)
        _ <- IO(map.put(handle, pager))
      } yield handle

      t.to[Task].liftM[MT].liftB
    }

    def more(h: ResultHandle): Backend[Vector[Data]] = {
      val t = for {
        pager <- Task.delay(Option(map.get(h)).get)
        chunk <- pager.more
      } yield chunk

      t.liftM[MT].liftB
    }

    def close(h: ResultHandle): Configured[Unit] = {
      val t = for {
        pager <- Task.delay(Option(map.get(h)).get)
        check <- Task.delay(map.remove(h, pager))
        _ <- if (check) pager.close else Task.now(())
      } yield ()

      t.liftM[MT].liftM[ConfiguredT]
    }

    def explain(repr: Repr): Backend[String] = "🤹".point[Backend]

    def listContents(dir: ADir): Backend[Set[PathSegment]] = {
      for {
        connectors <- cake[Backend]
        (precog, lwfs) = connectors

        existsPrecog <- precog.fs.exists(dir).liftM[MT].liftB

        backPrecog <- if (existsPrecog) {
          precog.fs.listContents(dir).map(_.some).liftM[MT].liftB
        } else {
          none[Set[PathSegment]].point[Backend]
        }

        backLwfs <- lwfs.children(dir).liftM[MT].liftB

        appended <- Task.delay(backPrecog |+| backLwfs).liftM[MT].liftB

        back <- appended match {
          case Some(segments) =>
            // if the dir exists in the lwc and/xor mimir, return the union of the results
            segments.point[Backend]
          case None =>
            // if the dir doesn't exist in either the lwc or mimir, raise an error
            MonadError_[Backend, FileSystemError].raiseError[Set[PathSegment]](pathErr(pathNotFound(dir)))
        }
      } yield back
    }

    def fileExists(file: AFile): Configured[Boolean] =
      for {
        connectors <- cake[Configured]
        (precog, lwfs) = connectors

        precogExists <- precog.fs.exists(file).liftM[MT].liftM[ConfiguredT]

        back <- if (precogExists)
          true.point[Configured]
        else
          lwfs.exists(file).liftM[MT].liftM[ConfiguredT]
      } yield back
  }

  object ReadFileModule extends ReadFileModule {
    import ReadFile._

    private type LWCData = (AtomicBoolean, Option[Stream[Task, Data]])

    private val readMap =
      new ConcurrentHashMap[ReadHandle, Precog#TablePager \/ LWCData]

    private val cur = new AtomicLong(0L)

    def open(file: AFile, offset: Natural, limit: Option[Positive]): Backend[ReadHandle] = {
      for {
        connectors <- cake[Backend]
        (precog, lwfs) = connectors

        handle <- Task.delay(ReadHandle(file, cur.getAndIncrement())).liftM[MT].liftB

        target = precog.Table.constString(Set(posixCodec.printPath(file)))

        // If the file doesn't exist in mimir we read from the lightweight connector.
        // FIXME apparently read on a non-existent file is equivalent to reading the empty file??!!
        eitherTable <- precog.Table.load(target, JType.JUniverseT).mapT(_.to[Task]).run.liftM[MT].liftB

        reader <- eitherTable.bitraverse(
          _ => {
            val dropped = lwfs.read(file).map(_.map((_.drop(offset.value))))
            limit.fold(dropped)(l => dropped.map(_.map(_.take(l.value))))
          },
          table => {
            val limited = if (offset.value === 0L && !limit.isDefined)
              table
            else
              table.takeRange(offset.value, limit.fold(slamdata.Predef.Int.MaxValue.toLong)(_.value))

            val projected = limited.transform(precog.trans.constants.SourceValue.Single)

            precog.TablePager(projected).to[Task]
          }).liftM[MT].liftB

        _ <- Task.delay {
          readMap.put(handle, reader.swap.map(new AtomicBoolean(false) -> _))
        }.liftM[MT].liftB
      } yield handle
    }

    def read(h: ReadHandle): Backend[Vector[Data]] = {
      for {
        maybeReader <- Task.delay(Option(readMap.get(h))).liftM[MT].liftB

        reader <- maybeReader match {
          case Some(reader) =>
            reader.point[Backend]
          case None =>
            MonadError_[Backend, FileSystemError].raiseError(unknownReadHandle(h))
        }

        chunk <- reader.fold(
          _.more,
          {
            case (bool, data) if !bool.getAndSet(true) =>
              // FIXME this pages the entire lwc dataset into memory, crashing the server
              data.fold(Vector[Data]().point[Task])(_.runLog)
            case (_, _) => // enqueue the empty vector so ReadFile.scan knows when to stop scanning
              Vector[Data]().point[Task]
          }).liftM[MT].liftB
      } yield chunk
    }

    def close(h: ReadHandle): Configured[Unit] = {
      val t = for {
        reader <- Task.delay(Option(readMap.get(h)).get)
        check <- Task.delay(readMap.remove(h, reader))
        _ <- if (check) reader.fold(_.close, _ => Task.now(())) else Task.now(())
      } yield ()

      t.liftM[MT].liftM[ConfiguredT]
    }
  }

  object WriteFileModule extends WriteFileModule {
    import WriteFile._

    // we set this to 1 because we don't want the table evaluation "running ahead" of
    // quasar's paging logic.  See also: TablePager.apply
    private val QueueLimit = 1

    private val map: ConcurrentHashMap[WriteHandle, (Queue[IO, Vector[Data]], Signal[IO, Boolean])] =
      new ConcurrentHashMap

    private val cur = new AtomicLong(0L)

    def open(file: AFile): Backend[WriteHandle] = {
      val run: Task[M[WriteHandle]] = Task.delay {
        log.debug(s"open file $file")

        val id = cur.getAndIncrement()
        val handle = WriteHandle(file, id)

        for {
          queue <- Queue.bounded[IO, Vector[Data]](QueueLimit)(asyncInstance).to[Task].liftM[MT]
          signal <- fs2.async.signalOf[IO, Boolean](false)(asyncInstance).to[Task].liftM[MT]

          path = fileToPath(file)
          jvs = queue.dequeue.takeWhile(_.nonEmpty).flatMap(Stream.emits).map(JValue.fromData)

          connectors <- cake[M]
          (precog, _) = connectors

          ingestion = (for {
            // TODO log resource errors?
            _ <- precog.ingest(path, jvs).run
            _ <- signal.set(true)
          } yield ()).to[Task]

          // run asynchronously forever
          _ <- startTask(ingestion, ()).liftM[MT]
          _ <- Task.delay(log.debug(s"Started ingest.")).liftM[MT]

          _ <- Task.delay(map.put(handle, (queue, signal))).liftM[MT]
        } yield handle
      }

      run.liftM[MT].join.liftB
    }

    def write(h: WriteHandle, chunk: Vector[Data]): Configured[Vector[FileSystemError]] = {
      log.debug(s"write to $h and $chunk")

      val t = for {
        maybePair <- IO(Option(map.get(h)))

        back <- maybePair match {
          case Some(pair) =>
            if (chunk.isEmpty) {
              IO.pure(Vector.empty[FileSystemError])
            } else {
              val (queue, _) = pair
              queue.enqueue1(chunk).map(_ => Vector.empty[FileSystemError])
            }

          case _ =>
            IO.pure(Vector(unknownWriteHandle(h)))
        }
      } yield back

      t.to[Task].liftM[MT].liftM[ConfiguredT]
    }

    def close(h: WriteHandle): Configured[Unit] = {
      val t = for {
        // yolo we crash because quasar
        pair <- IO(Option(map.get(h)).get)
        (queue, signal) = pair

        _ <- IO(map.remove(h))
        _ <- IO(log.debug(s"close $h"))
        // ask queue to stop
        _ <- queue.enqueue1(Vector.empty)
        // wait until queue actually stops; task async completes when signal completes
        _ <- signal.discrete.takeWhile(!_).run
      } yield ()

      t.to[Task].liftM[MT].liftM[ConfiguredT]
    }
  }

  object ManageFileModule extends ManageFileModule {
    import ManageFile._

    // TODO directory moving and varying semantics
    def move(scenario: PathPair, semantics: MoveSemantics): Backend[Unit] = {
      scenario.fold(
        d2d = { (from, to) =>
          for {
            connectors <- cake[Backend]
            (precog, _) = connectors

            exists <- precog.fs.exists(from).liftM[MT].liftB

            _ <- if (exists)
              ().point[Backend]
            else
              MonadError_[Backend, FileSystemError].raiseError(pathErr(pathNotFound(from)))

            result <- precog.fs.moveDir(from, to, semantics).liftM[MT].liftB

            _ <- if (result) {
              ().point[Backend]
            } else {
              val error = semantics match {
                case MoveSemantics.FailIfMissing => pathNotFound(to)
                case _ => pathExists(to)
              }

              MonadError_[Backend, FileSystemError].raiseError(pathErr(error))
            }
          } yield ()
        },
        f2f = { (from, to) =>
          for {
            connectors <- cake[Backend]
            (precog, _) = connectors

            exists <- precog.fs.exists(from).liftM[MT].liftB

            _ <- if (exists)
              ().point[Backend]
            else
              MonadError_[Backend, FileSystemError].raiseError(pathErr(pathNotFound(from)))

            result <- precog.fs.moveFile(from, to, semantics).liftM[MT].liftB

            _ <- if (result) {
              ().point[Backend]
            } else {
              val error = semantics match {
                case MoveSemantics.FailIfMissing => pathNotFound(to)
                case _ => pathExists(to)
              }

              MonadError_[Backend, FileSystemError].raiseError(pathErr(error))
            }
          } yield ()
        })
    }

    def copy(pair: PathPair): Backend[Unit] =
      MonadError_[Backend, FileSystemError].raiseError(unsupportedOperation("Mimir currently does not support copy"))

    def delete(path: APath): Backend[Unit] = {
      for {
        connectors <- cake[Backend]
        (precog, _) = connectors

        exists <- precog.fs.exists(path).liftM[MT].liftB

        _ <- if (exists)
          ().point[Backend]
        else
          MonadError_[Backend, FileSystemError].raiseError(pathErr(pathNotFound(path)))

        _ <- precog.fs.delete(path).liftM[MT].liftB
      } yield ()
    }

    val defaultPrefix = TempFilePrefix("")

    def tempFile(near: APath, prefix: Option[TempFilePrefix]): Backend[AFile] = {
      for {
        uuid <- Task.delay(UUID.randomUUID().toString).liftM[MT].liftB
      } yield TmpFile.tmpFile0(near, prefix.getOrElse(defaultPrefix), uuid)
    }
  }
}

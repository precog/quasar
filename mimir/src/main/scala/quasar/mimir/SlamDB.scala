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
import quasar.common._
import quasar.connector._
import quasar.contrib.pathy._
import quasar.contrib.scalaz._, eitherT._
import quasar.fp._
import quasar.fp.numeric._
import quasar.fs._
import quasar.fs.mount._
import quasar.mimir.MimirCake._
import quasar.qscript._
import quasar.qscript.analysis._
import quasar.qscript.rewrites.{Optimize, Unicoalesce, Unirewrite}

import quasar.blueeyes.json.JValue
import quasar.precog.common.Path
import quasar.yggdrasil.bytecode.JType

import delorean._

import fs2.{async, Stream}
import fs2.async.mutable.{Queue, Signal}
import fs2.interop.scalaz._

import matryoshka._
import matryoshka.implicits._
import matryoshka.data._

import org.slf4s.Logging

import pathy.Path._

import scalaz._, Scalaz._
import scalaz.concurrent.Task

import scala.Predef.implicitly
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

trait SlamDB extends BackendModule with Logging with DefaultAnalyzeModule {
  import FileSystemError._
  import PathError._
  import Precog.startTask

  val lwc: LightweightConnector

  type QS[T[_[_]]] = MimirQScriptCP[T]

  implicit def qScriptToQScriptTotal[T[_[_]]]: Injectable.Aux[QSM[T, ?], QScriptTotal[T, ?]] =
    mimir.qScriptToQScriptTotal[T]

  implicit def qScriptCoreToQScript[T[_[_]]]: Injectable.Aux[QScriptCore[T, ?], QSM[T, ?]] =
    Injectable.inject[QScriptCore[T, ?], QSM[T, ?]]

  implicit def equiJoinToQScript[T[_[_]]]: Injectable.Aux[EquiJoin[T, ?], QSM[T, ?]] =
    Injectable.inject[EquiJoin[T, ?], QSM[T, ?]]

  implicit def shiftedReadToQScript[T[_[_]]]: Injectable.Aux[Const[ShiftedRead[AFile], ?], QSM[T, ?]] =
    Injectable.inject[Const[ShiftedRead[AFile], ?], QSM[T, ?]]

  type Repr = MimirRepr
  type MT[F[_], A] = CakeMT[F, lwc.FS, A]
  type M[A] = CakeM[lwc.FS, A]

  import Cost._
  import Cardinality._

  def CardinalityQSM: Cardinality[QSM[Fix, ?]] = Cardinality[QSM[Fix, ?]]
  def CostQSM: Cost[QSM[Fix, ?]] = Cost[QSM[Fix, ?]]
  def FunctorQSM[T[_[_]]] = Functor[QSM[T, ?]]
  def TraverseQSM[T[_[_]]] = Traverse[QSM[T, ?]]
  def DelayRenderTreeQSM[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = implicitly[Delay[RenderTree, QSM[T, ?]]]
  def ExtractPathQSM[T[_[_]]: RecursiveT] = ExtractPath[QSM[T, ?], APath]
  def QSCoreInject[T[_[_]]] = implicitly[QScriptCore[T, ?] :<: QSM[T, ?]]
  def MonadM = Monad[M]
  def UnirewriteT[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = implicitly[Unirewrite[T, QS[T]]]
  def UnicoalesceCap[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = Unicoalesce.Capture[T, QS[T]]

  final case class Config(dataDir: java.io.File)

  def optimize[T[_[_]]: BirecursiveT: EqualT: ShowT]
      : QSM[T, T[QSM[T, ?]]] => QSM[T, T[QSM[T, ?]]] = {
    val O = new Optimize[T]
    O.optimize(reflNT[QSM[T, ?]])
  }

  def parseConfig(uri: ConnectionUri): BackendDef.DefErrT[Task, Config] = {
    val file = new java.io.File(uri.value)

    if (!file.isAbsolute)
      EitherT.leftT(NonEmptyList("Mimir cannot be mounted to a relative path").left.point[Task])
    else
      Config(file).point[BackendDef.DefErrT[Task, ?]]
  }

  def compile(cfg: Config): BackendDef.DefErrT[Task, (M ~> Task, Task[Unit])] = {
    val t = for {
      cake <- Precog(cfg.dataDir)
      connector <- lwc.init
    } yield {
      val (fs, shutdown) = connector
      (λ[M ~> Task](_.run((cake: Cake, fs))), cake.shutdown.toTask >> shutdown)
    }

    t.liftM[BackendDef.DefErrT]
  }

  val Type: FileSystemType = lwc.Type

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

    def qScriptCorePlanner = new QScriptCorePlanner[T, Backend, lwc.FS](
      λ[Task ~> Backend](_.liftM[MT].liftB),
      λ[M ~> Backend](_.liftB))

    def equiJoinPlanner = new EquiJoinPlanner[T, Backend](
      λ[Task ~> Backend](_.liftM[MT].liftB))

    val liftErr: FileSystemErrT[M, ?] ~> Backend =
      Hoist[FileSystemErrT].hoist[M, PhaseResultT[Configured, ?]](
        λ[Configured ~> PhaseResultT[Configured, ?]](_.liftM[PhaseResultT])
          compose λ[M ~> Configured](_.liftM[ConfiguredT]))

    def shiftedReadPlanner = new ShiftedReadPlanner[T, Backend, lwc.FS](liftErr)

    lazy val planQST: AlgebraM[Backend, QScriptTotal[T, ?], Repr] =
      _.run.fold(
        qScriptCorePlanner.plan(planQST),
        _.run.fold(
          _ => ???,   // ProjectBucket
          _.run.fold(
            _ => ???,   // ThetaJoin
            _.run.fold(
              equiJoinPlanner.plan(planQST),
              _.run.fold(
                _ => ???,    // ShiftedRead[ADir]
                _.run.fold(
                  shiftedReadPlanner.plan,
                  _.run.fold(
                    _ => ???,   // Read[ADir]
                    _.run.fold(
                      _ => ???,   // Read[AFile]
                      _ => ???))))))))    // DeadEnd

    def planQSM(in: QSM[T, Repr]): Backend[Repr] =
      in.run.fold(qScriptCorePlanner.plan(planQST), _.run.fold(
        equiJoinPlanner.plan(planQST),
	shiftedReadPlanner.plan))

    cp.cataM(planQSM _)
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
        q <- async.boundedQueue[Task, Vector[JValue]](1)

        populator = repr.table.slices.trans(λ[Future ~> Task](_.toTask)) foreachRec { slice =>
          if (!slice.isEmpty) {
            val json = slice.toJsonElements
            if (!json.isEmpty)
              q.enqueue1(json)
            else
              Task.now(())
          } else {
            Task.now(())
          }
        }

        populatorWithTermination = populator >> q.enqueue1(Vector.empty)

        ingestor = repr.P.ingest(path, q.dequeue.takeWhile(_.nonEmpty).flatMap(Stream.emits)).run

        // generally this function is bad news (TODO provide a way to ingest as a Stream)
        _ <- Task.gatherUnordered(Seq(populatorWithTermination, ingestor))
      } yield ()

      driver.liftM[MT].liftB
    }

    def evaluatePlan(repr: Repr): Backend[ResultHandle] = {
      val t = for {
        handle <- Task.delay(ResultHandle(cur.getAndIncrement()))
        pager <- repr.P.TablePager(repr.table)
        _ <- Task.delay(map.put(handle, pager))
      } yield handle

      t.liftM[MT].liftB
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
        connectors <- cake[Backend, lwc.FS]
        (precog, lwfs) = connectors

        existsPrecog <- precog.fs.exists(dir).liftM[MT].liftB
        existsLwfs <- lwfs.exists(dir).liftM[MT].liftB

        _ <- if (existsPrecog || existsLwfs)
          ().point[Backend]
        else
          MonadError_[Backend, FileSystemError].raiseError(pathErr(pathNotFound(dir)))

        backPrecog <- if (existsPrecog)
          precog.fs.listContents(dir).liftM[MT].liftB
        else
          Set[PathSegment]().point[Backend]

        backLwfs <- if (existsLwfs)
          lwfs.children(dir).liftM[MT].liftB
        else
          Set[PathSegment]().point[Backend]

      } yield backPrecog ++ backLwfs
    }

    // TODO keep these old impls around and private
    // have a new imple that calls that and is the real impl

    // redefine def cake so we can auto lift into the tuple, or something like that
    // for all the cases where we don't need to delegate

    def fileExists(file: AFile): Configured[Boolean] =
      cake[M, lwc.FS].flatMap {
        case (precog, lwfs) =>
          ((precog.fs.exists(file) |@| lwfs.exists(file))(_ || _)).liftM[MT]
      }.liftM[ConfiguredT]
  }

  object ReadFileModule extends ReadFileModule {
    import ReadFile._

    private val readMap =
      new ConcurrentHashMap[ReadHandle, Precog#TablePager \/ Option[Stream[Task, Data]]]

    private val cur = new AtomicLong(0L)

    def open(file: AFile, offset: Natural, limit: Option[Positive]): Backend[ReadHandle] = {
      for {
        connectors <- cake[Backend, lwc.FS]
        (precog, lwfs) = connectors

        handle <- Task.delay(ReadHandle(file, cur.getAndIncrement())).liftM[MT].liftB

        target = precog.Table.constString(Set(posixCodec.printPath(file)))

        // apparently read on a non-existent file is equivalent to reading the empty file??!!
        // if the file doesn't exist in mimir we delegate to the lightweight connector
        eitherTable <- precog.Table.load(target, JType.JUniverseT).mapT(_.toTask).run.liftM[MT].liftB

        reader <- eitherTable.bitraverse(
          _ => lwfs.read(file),
          table => {
            val limited = if (offset.value === 0L && !limit.isDefined)
              table
            else
              table.takeRange(offset.value, limit.fold(slamdata.Predef.Int.MaxValue.toLong)(_.value))

            val projected = limited.transform(precog.trans.constants.SourceValue.Single)

            precog.TablePager(projected)
          }).liftM[MT].liftB

        _ <- Task.delay(readMap.put(handle, reader.swap)).liftM[MT].liftB
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

        // #unreadable
        chunk <- reader.fold(_.more, _.fold(Task.now(Vector[Data]()))(_.runLog)).liftM[MT].liftB
      } yield chunk
    }

    // TODO close stuff for lwc?
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

    private val map: ConcurrentHashMap[WriteHandle, (Queue[Task, Vector[Data]], Signal[Task, Boolean])] =
      new ConcurrentHashMap

    private val cur = new AtomicLong(0L)

    def open(file: AFile): Backend[WriteHandle] = {
      val run: Task[M[WriteHandle]] = Task delay {
        log.debug(s"open file $file")

        val id = cur.getAndIncrement()
        val handle = WriteHandle(file, id)

        for {
          queue <- Queue.bounded[Task, Vector[Data]](QueueLimit).liftM[MT]
          signal <- fs2.async.signalOf[Task, Boolean](false).liftM[MT]

          path = fileToPath(file)
          jvs = queue.dequeue.takeWhile(_.nonEmpty).flatMap(Stream.emits).map(JValue.fromData)

          connectors <- cake[M, lwc.FS]
          (precog, _) = connectors

          ingestion = for {
            _ <- precog.ingest(path, jvs).run   // TODO log resource errors?
            _ <- signal.set(true)
          } yield ()

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
        maybePair <- Task.delay(Option(map.get(h)))

        back <- maybePair match {
          case Some(pair) =>
            if (chunk.isEmpty) {
              Task.now(Vector.empty[FileSystemError])
            } else {
              val (queue, _) = pair
              queue.enqueue1(chunk).map(_ => Vector.empty[FileSystemError])
            }

          case _ =>
            Task.now(Vector(unknownWriteHandle(h)))
        }
      } yield back

      t.liftM[MT].liftM[ConfiguredT]
    }

    def close(h: WriteHandle): Configured[Unit] = {
      val t = for {
        // yolo we crash because quasar
        pair <- Task.delay(Option(map.get(h)).get).liftM[MT]
        (queue, signal) = pair

        _ <- Task.delay(map.remove(h)).liftM[MT]
        _ <- Task.delay(log.debug(s"close $h")).liftM[MT]
        // ask queue to stop
        _ <- queue.enqueue1(Vector.empty).liftM[MT]
        // wait until queue actually stops; task async completes when signal completes
        _ <- signal.discrete.takeWhile(!_).run.liftM[MT]
      } yield ()

      t.liftM[ConfiguredT]
    }
  }

  object ManageFileModule extends ManageFileModule {
    import ManageFile._

    // TODO directory moving and varying semantics
    def move(scenario: PathPair, semantics: MoveSemantics): Backend[Unit] = {
      scenario.fold(
        d2d = { (from, to) =>
          for {
            connectors <- cake[Backend, lwc.FS]
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
            connectors <- cake[Backend, lwc.FS]
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
        connectors <- cake[Backend, lwc.FS]
        (precog, _) = connectors

        exists <- precog.fs.exists(path).liftM[MT].liftB

        _ <- if (exists)
          ().point[Backend]
        else
          MonadError_[Backend, FileSystemError].raiseError(pathErr(pathNotFound(path)))

        _ <- precog.fs.delete(path).liftM[MT].liftB
      } yield ()
    }

    def tempFile(near: APath): Backend[AFile] = {
      for {
        seed <- Task.delay(UUID.randomUUID().toString).liftM[MT].liftB
      } yield refineType(near).fold(p => p, fileParent) </> file(seed)
    }
  }
}

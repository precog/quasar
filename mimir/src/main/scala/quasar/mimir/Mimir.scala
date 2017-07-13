/*
 * Copyright 2014–2017 SlamData Inc.
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
import quasar.fp.ski.κ
import quasar.fs._
import quasar.fs.mount._
import quasar.qscript._

import quasar.blueeyes.json.{JNum, JValue}
import quasar.precog.common.Path
import quasar.yggdrasil.bytecode.JType

import fs2.{async, Stream}
import fs2.async.mutable.{Queue, Signal}
import fs2.interop.scalaz._

import matryoshka._
import matryoshka.implicits._
import matryoshka.data._
import matryoshka.patterns._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

import org.slf4s.Logging

import pathy.Path._

import delorean._

import scala.Predef.implicitly
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

object Mimir extends BackendModule with Logging {
  import FileSystemError._
  import PathError._

  // pessimistically equal to couchbase's
  type QS[T[_[_]]] =
    QScriptCore[T, ?] :\:
    EquiJoin[T, ?] :/:
    Const[ShiftedRead[AFile], ?]

  implicit def qScriptToQScriptTotal[T[_[_]]]: Injectable.Aux[QSM[T, ?], QScriptTotal[T, ?]] =
    ::\::[QScriptCore[T, ?]](::/::[T, EquiJoin[T, ?], Const[ShiftedRead[AFile], ?]])

  private type Cake = Precog with Singleton

  trait Repr {
    type P <: Cake

    val P: P
    val table: P.Table

    /**
     * Given a term which is a type derived from the generic cake,
     * produce the equivalent term which is typed specific to the
     * cake in this repr.  For example:
     *
     * {{{
     * val transM: M[Cake#trans#TransSpec1] = cake.flatMap(_.trans.Single.Value)
     *
     * def foo(repr: Repr) = for {
     *   trans <- repr.unsafeCoerce[λ[`P <: Cake` => P.trans.TransSpec1]](transM).liftM[MT]
     * } yield repr.table.transform(trans)
     * }}}
     */
    def unsafeCoerce[F[_ <: Cake]](term: F[Cake]): Task[F[P]] =
      Task.delay(term.asInstanceOf[F[P]])

    def map(f: P.Table => P.Table): Repr { type P = Repr.this.P.type } =
      Repr(P)(f(table))
  }

  object Repr {
    def apply(P0: Precog)(table0: P0.Table): Repr { type P = P0.type } =
      new Repr {
        type P = P0.type

        val P: P = P0
        val table: P.Table = table0
      }

    def meld[F[_]: Monad](fn: DepFn1[Cake, λ[`P <: Cake` => F[P#Table]]])(
      implicit
        F: MonadReader_[F, Cake]): F[Repr] =
      F.ask.flatMap(cake => fn(cake).map(table => Repr(cake)(table)))
  }

  private type MT[F[_], A] = Kleisli[F, Cake, A]
  type M[A] = MT[Task, A]

  def cake[F[_]](implicit F: MonadReader_[F, Cake]): F[Cake] = F.ask

  def FunctorQSM[T[_[_]]] = Functor[QSM[T, ?]]
  def DelayRenderTreeQSM[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = implicitly[Delay[RenderTree, QSM[T, ?]]]
  def ExtractPathQSM[T[_[_]]: RecursiveT] = ExtractPath[QSM[T, ?], APath]
  def QSCoreInject[T[_[_]]] = implicitly[QScriptCore[T, ?] :<: QSM[T, ?]]
  def MonadM = Monad[M]
  def UnirewriteT[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = implicitly[Unirewrite[T, QS[T]]]
  def UnicoalesceCap[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = Unicoalesce.Capture[T, QS[T]]

  final case class Config(dataDir: java.io.File)

  def parseConfig(uri: ConnectionUri): FileSystemDef.DefErrT[Task, Config] =
    Config(new java.io.File("/tmp/precog")).point[FileSystemDef.DefErrT[Task, ?]]

  def compile(cfg: Config): FileSystemDef.DefErrT[Task, (M ~> Task, Task[Unit])] = {
    val t = for {
      cake <- Precog(cfg.dataDir)
    } yield (λ[M ~> Task](_.run(cake)), cake.shutdown.toTask)

    t.liftM[FileSystemDef.DefErrT]
  }

  val Type = FileSystemType("mimir")

  def plan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](
      cp: T[QSM[T, ?]]): Backend[Repr] = {

// M = Backend
// F[_] = MapFuncCore[T, ?]
// B = Repr
// A = SrcHole
// AlgebraM[M, CoEnv[A, F, ?], B] = AlgebraM[Backend, CoEnv[Hole, MapFuncCore[T, ?], ?], Repr]
//def interpretM[M[_], F[_], A, B](f: A => M[B], φ: AlgebraM[M, F, B]): AlgebraM[M, CoEnv[A, F, ?], B]
// f.cataM(interpretM)

    val mapFuncPlanner = new MapFuncPlanner[T, M, Backend]

    lazy val planQST: AlgebraM[Backend, QScriptTotal[T, ?], Repr] =
      _.run.fold(
        planQScriptCore,
        _.run.fold(
          _ => ???,   // ProjectBucket
          _.run.fold(
            _ => ???,   // ThetaJoin
            _.run.fold(
              planEquiJoin,
              _.run.fold(
                _ => ???,    // ShiftedRead[ADir]
                _.run.fold(
                  planShiftedRead,
                  _.run.fold(
                    _ => ???,   // Read[ADir]
                    _.run.fold(
                      _ => ???,   // Read[AFile]
                      _ => ???))))))))    // DeadEnd

    lazy val planQScriptCore: AlgebraM[Backend, QScriptCore[T, ?], Repr] = {
      case qscript.Map(src, f) =>
        for {
          trans <- f.cataM[Backend, src.P.trans.TransSpec1](
            interpretM(
              κ(src.P.trans.TransSpec1.Id.point[Backend]),
              mapFuncPlanner.plan(src.P)))
        } yield Repr(src.P)(src.table.transform(trans))

      case qscript.LeftShift(src, struct, id, repair) => ???
      case qscript.Reduce(src, bucket, reducers, repair) => ???
      case qscript.Sort(src, bucket, order) => ???

      case qscript.Filter(src, f) =>
        for {
          trans <- f.cataM[Backend, src.P.trans.TransSpec1](
            interpretM(
              κ(src.P.trans.TransSpec1.Id.point[Backend]),
              mapFuncPlanner.plan(src.P)))
        } yield Repr(src.P)(src.table.transform(src.P.trans.Filter(src.P.trans.TransSpec1.Id, trans)))

      case qscript.Union(src, lBranch, rBranch) => ???
        //for {
        //  leftRepr <- lBranch.cataM(interpretM(κ(src.point[Backend]), planQST))
        //  rightRepr <- rBranch.cataM(interpretM(κ(src.point[Backend]), planQST))
        //  rightCoerced <- leftRepr.unsafeCoerce[Lambda[`P <: Cake` => P#Table]](rightRepr.table).liftM[MT].liftB
        //} yield Repr(leftRepr.P)(leftRepr.table.concat(rightCoerced))

      case qscript.Subset(src, from, op, count) =>
        for {
          fromRepr <- from.cataM(interpretM(κ(src.point[Backend]), planQST))
          countRepr <- count.cataM(interpretM(κ(src.point[Backend]), planQST))
          back <- {
            def result = for {
              vals <- countRepr.table.toJson
              nums = vals collect { case n: JNum => n.toLong.toInt } // TODO error if we get something strange
              number = nums.head
              compacted = fromRepr.table.compact(fromRepr.P.trans.TransSpec1.Id)
              back <- op match {
                case Take =>
                  Future.successful(compacted.takeRange(0, number))

                case Drop =>
                  Future.successful(compacted.takeRange(number, slamdata.Predef.Int.MaxValue.toLong)) // blame precog

                case Sample =>
                  compacted.sample(number, List(fromRepr.P.trans.TransSpec1.Id)).map(_.head) // the number of Reprs returned equals the number of transspecs
              }
            } yield Repr(fromRepr.P)(back)

            result.toTask.liftM[MT].liftB
          }
        } yield back

      // FIXME look for Map(Unreferenced, Constant) and return constant table
      case qscript.Unreferenced() =>
        Repr.meld[M](new DepFn1[Cake, λ[`P <: Cake` => M[P#Table]]] {
          def apply(P: Cake): M[P.Table] =
            P.Table.constLong(Set(0)).point[M]
        }).liftB
    }

    lazy val planEquiJoin: AlgebraM[Backend, EquiJoin[T, ?], Repr] = _ => ???

    lazy val planShiftedRead: AlgebraM[Backend, Const[ShiftedRead[AFile], ?], Repr] = {
      case Const(ShiftedRead(path, status)) => {
        val pathStr: String = pathy.Path.posixCodec.printPath(path)

        val loaded: EitherT[M, FileSystemError, Repr] =
          for {
            precog <- cake[EitherT[M, FileSystemError, ?]]
            apiKey <- precog.RootAPIKey.toTask.liftM[MT].liftM[EitherT[?[_], FileSystemError, ?]]

            repr <-
              Repr.meld[EitherT[M, FileSystemError, ?]](
                new DepFn1[Cake, λ[`P <: Cake` => EitherT[M, FileSystemError, P#Table]]] {
                  def apply(P: Cake): EitherT[M, FileSystemError, P.Table] = {
                    val et =
                      P.Table.constString(Set(pathStr)).load(apiKey, JType.JUniverseT).mapT(_.toTask)

                    et.mapT(_.liftM[MT]) leftMap { err =>
                      val msg = err.messages.toList.reduce(_ + ";" + _)
                      readFailed(posixCodec.printPath(path), msg)
                    }
                  }
                })
          } yield {
            status match {
              case IdOnly => repr.map(_.transform(repr.P.trans.constants.SourceKey.Single))
              case IncludeId => repr
              case ExcludeId => repr.map(_.transform(repr.P.trans.constants.SourceValue.Single))
            }
          }

        val result: FileSystemErrT[M, ?] ~> Backend =
          Hoist[FileSystemErrT].hoist[M, PhaseResultT[Configured, ?]](
            λ[Configured ~> PhaseResultT[Configured, ?]](_.liftM[PhaseResultT])
              compose λ[M ~> Configured](_.liftM[ConfiguredT]))

        result(loaded)
      }
    }

    def planQSM(in: QSM[T, Repr]): Backend[Repr] =
      in.run.fold(planQScriptCore, _.run.fold(planEquiJoin, planShiftedRead))

    cp.cataM(planQSM _)
  }

  private def dequeueStreamT[F[_]: Functor, A](q: Queue[F, A])(until: A => Boolean): StreamT[F, A] = {
    StreamT.unfoldM[F, A, Queue[F, A]](q) { q =>
      q.dequeue1 map { a =>
        if (until(a))
          None
        else
          Some((a, q))
      }
    }
  }

  private def dirToPath(dir: ADir): Path = Path(pathy.Path.posixCodec.printPath(dir))
  private def fileToPath(file: AFile): Path = Path(pathy.Path.posixCodec.printPath(file))

  object QueryFileModule extends QueryFileModule {
    import QueryFile._

    private val map = new ConcurrentHashMap[ResultHandle, Precog#TablePager]
    private val cur = new AtomicLong(0L)

    def executePlan(repr: Repr, out: AFile): Backend[AFile] = {
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

      driver.map(_ => out).liftM[MT].liftB
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
        precog <- cake[Backend]

        exists <- precog.fs.exists(dir).liftM[MT].liftB

        _ <- if (exists)
          ().point[Backend]
        else
          MonadError_[Backend, FileSystemError].raiseError(pathErr(pathNotFound(dir)))

        back <- precog.fs.listContents(dir).liftM[MT].liftB
      } yield back
    }

    def fileExists(file: AFile): Configured[Boolean] =
      cake[M].flatMap(_.fs.exists(file).liftM[MT]).liftM[ConfiguredT]
  }

  object ReadFileModule extends ReadFileModule {
    import ReadFile._

    private val map = new ConcurrentHashMap[ReadHandle, Precog#TablePager]
    private val cur = new AtomicLong(0L)

    def open(file: AFile, offset: Natural, limit: Option[Positive]): Backend[ReadHandle] = {
      for {
        precog <- cake[Backend]
        handle <- Task.delay(ReadHandle(file, cur.getAndIncrement())).liftM[MT].liftB

        target = precog.Table.constString(Set(posixCodec.printPath(file)))

        // apparently read on a non-existent file is equivalent to reading the empty file??!!
        eitherTable <- precog.Table.load(target, JType.JUniverseT).mapT(_.toTask).run.liftM[MT].liftB
        table = eitherTable.fold(_ => precog.Table.empty, table => table)

        limited = if (offset.value === 0L && !limit.isDefined)
          table
        else
          table.takeRange(offset.value, limit.fold(slamdata.Predef.Int.MaxValue.toLong)(_.value))

        projected = limited.transform(precog.trans.constants.SourceValue.Single)

        pager <- precog.TablePager(projected).liftM[MT].liftB
        _ <- Task.delay(map.put(handle, pager)).liftM[MT].liftB
      } yield handle
    }

    def read(h: ReadHandle): Backend[Vector[Data]] = {
      for {
        maybePager <- Task.delay(Option(map.get(h))).liftM[MT].liftB

        pager <- maybePager match {
          case Some(pager) =>
            pager.point[Backend]

          case None =>
            MonadError_[Backend, FileSystemError].raiseError(unknownReadHandle(h))
        }

        chunk <- pager.more.liftM[MT].liftB
      } yield chunk
    }

    def close(h: ReadHandle): Configured[Unit] = {
      val t = for {
        pager <- Task.delay(Option(map.get(h)).get)
        check <- Task.delay(map.remove(h, pager))
        _ <- if (check) pager.close else Task.now(())
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

          precog <- cake[M]

          ingestion = for {
            _ <- precog.ingest(path, jvs).run   // TODO log resource errors?
            _ <- signal.set(true)
          } yield ()

          // run asynchronously forever
          _ <- Task.delay(ingestion.unsafePerformAsync(_ => ())).liftM[MT]
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
    def move(scenario: MoveScenario, semantics: MoveSemantics): Backend[Unit] = {
      scenario.fold(
        d2d = { (from, to) =>
          for {
            precog <- cake[Backend]

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
            precog <- cake[Backend]

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

    def delete(path: APath): Backend[Unit] = {
      for {
        precog <- cake[Backend]

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

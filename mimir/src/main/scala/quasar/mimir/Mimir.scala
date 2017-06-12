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
import quasar.precog.common.{Path, RValue}
import quasar.precog.util.IOUtils
import quasar.yggdrasil.PathMetadata
import quasar.yggdrasil.vfs.ResourceError
import quasar.yggdrasil.bytecode.JType

import fs2.async.mutable.{Queue, Signal}
import fs2.interop.scalaz._
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
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
import scala.util.Random

object Mimir extends BackendModule with Logging {

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
  def MonadFsErrM = ???
  def PhaseResultTellM = ???
  def PhaseResultListenM = ???
  def UnirewriteT[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = implicitly[Unirewrite[T, QS[T]]]
  def UnicoalesceCap[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = Unicoalesce.Capture[T, QS[T]]

  final case class Config(dataDir: java.io.File)

  def parseConfig(uri: ConnectionUri): FileSystemDef.DefErrT[Task, Config] =
    Config(new java.io.File("/tmp/precog")).point[FileSystemDef.DefErrT[Task, ?]]

  def compile(cfg: Config): FileSystemDef.DefErrT[Task, (M ~> Task, Task[Unit])] = {
    val t = for {
      cake <- Task.delay(new Precog(cfg.dataDir))
    } yield (λ[M ~> Task](_.run(cake)), cake.shutdown.toTask)

    t.liftM[FileSystemDef.DefErrT]
  }

  val Type = FileSystemType("mimir")

  def plan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](
      cp: T[QSM[T, ?]]): Backend[Repr] = {

// M = Backend
// F[_] = MapFunc[T, ?]
// B = Repr
// A = SrcHole
// AlgebraM[M, CoEnv[A, F, ?], B] = AlgebraM[Backend, CoEnv[Hole, MapFunc[T, ?], ?], Repr]
//def interpretM[M[_], F[_], A, B](f: A => M[B], φ: AlgebraM[M, F, B]): AlgebraM[M, CoEnv[A, F, ?], B]
// f.cataM(interpretM)

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

    lazy val planMapFunc: AlgebraM[Backend, MapFunc[T, ?], Repr] = {
      // EJson => Data => JValue => RValue => Table
      case MapFuncs.Constant(ejson) =>
        val data: Data = ejson.cata(Data.fromEJson)
        val jvalue: JValue = JValue.fromData(data)
        val rvalue: RValue = RValue.fromJValue(jvalue)

        Repr.meld[M](new DepFn1[Cake, λ[`P <: Cake` => M[P#Table]]] {
          def apply(P: Cake): M[P.Table] =
            P.Table.fromRValues(scala.Stream(rvalue)).point[M]
        }).liftB

      case _ => ???
    }

    lazy val planQScriptCore: AlgebraM[Backend, QScriptCore[T, ?], Repr] = {
      case qscript.Map(src, f) => f.cataM(interpretM(κ(src.point[Backend]), planMapFunc))
      case qscript.LeftShift(src, struct, id, repair) => ???
      case qscript.Reduce(src, bucket, reducers, repair) => ???
      case qscript.Sort(src, bucket, order) => ???
      case qscript.Filter(src, f) => ???
      case qscript.Union(src, lBranch, rBranch) => ???
      case qscript.Subset(src, from, op, count) =>
        for {
          fromRepr <- from.cataM(interpretM(κ(src.point[Backend]), planQST))
          countRepr <- count.cataM(interpretM(κ(src.point[Backend]), planQST))
          back <- {
            def result = for {
              vals <- countRepr.table.toJson
              nums = vals collect { case n: JNum => n.toLong.toInt } // TODO error if we get something strange
              number = nums.head
              back <- op match {
                case Take =>
                  Future.successful(fromRepr.table.takeRange(0, number))

                case Drop =>
                  Future.successful(fromRepr.table.takeRange(number, slamdata.Predef.Int.MaxValue.toLong)) // blame precog

                case Sample =>
                  fromRepr.table.sample(number, List(fromRepr.P.trans.TransSpec1.Id)).map(_.head) // the number of Reprs returned equals the number of transspecs
              }
            } yield Repr(fromRepr.P)(back)

            result.toTask.liftM[MT].liftB
          }
        } yield back

      case qscript.Unreferenced() =>
        Repr.meld[M](new DepFn1[Cake, λ[`P <: Cake` => M[P#Table]]] {
          def apply(P: Cake): M[P.Table] =
            P.Table.empty.point[M]
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

                    et.mapT(_.liftM[MT]).leftMap(toFSError)
                  }
                })
          } yield {
            status match {
              case IdOnly => repr.map(_.transform(repr.P.trans.constants.SourceKey.Single))
              case IncludeId => repr
              case ExcludeId => repr.map(_.transform(repr.P.trans.constants.SourceValue.Single))
            }
          }

        // TODO duplicated in listContents
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

  private def toSegment: PathMetadata => PathSegment = {
    // ShiftedRead[ADir]
    case PathMetadata(path, PathMetadata.DataDir(_)) =>
      DirName(path.components.mkString("/")).left[FileName]

    // ShiftedRead[AFile]
    case PathMetadata(path, PathMetadata.DataOnly(_)) =>
      FileName(path.components.mkString("/")).right[DirName]

    case PathMetadata(path, PathMetadata.PathOnly) =>
      DirName(path.components.mkString("/")).left[FileName]
  }

  private def children(dir: ADir): EitherT[M, ResourceError, Set[PathSegment]] = {
    log.debug(s"children of $dir")

    cake[M].liftM[EitherT[?[_], ResourceError, ?]] flatMap { P =>
      P.showContents(dirToPath(dir)).map(_.map(toSegment)).mapT(_.toTask.liftM[MT])
    }
  }

  private def toFSError: ResourceError => FileSystemError = {
    case ResourceError.Corrupt(msg) => ???
    case ResourceError.IOError(ex) => ???
    case ResourceError.IllegalWriteRequestError(msg) => ???
    case ResourceError.PermissionsError(msg) => ???
    case ResourceError.NotFound(msg) => ???
    case ResourceError.ResourceErrors(errs) => ???
  }

  object QueryFileModule extends QueryFileModule {
    import QueryFile._

    def executePlan(repr: Repr, out: AFile): Backend[AFile] = sys.error("woodle")

    private val map = new ConcurrentHashMap[ResultHandle, Vector[Data]]
    private val cur = new AtomicLong(0L)

    def evaluatePlan(repr: Repr): Backend[ResultHandle] = {
      val t = for {
        results <- repr.table.toJson.toTask     // dear god delete me please!!! halp alissa halp
        handle <- Task.delay(ResultHandle(cur.getAndIncrement()))
        _ <- Task.delay(map.put(handle, results.toVector.map(JValue.toData)))
      } yield handle

      t.liftM[MT].liftB
    }

    def more(h: ResultHandle): Backend[Vector[Data]] = {
      val t = for {
        back <- Task.delay(map.get(h))
        _ <- Task.delay(map.put(h, Vector.empty))
      } yield back

      t.liftM[MT].liftB
    }

    def close(h: ResultHandle): Configured[Unit] =
      (Task delay { map.remove(h); () }).liftM[MT].liftM[ConfiguredT]

    def explain(repr: Repr): Backend[String] = ???

    def listContents(dir: ADir): Backend[Set[PathSegment]] = {
      val segments: FileSystemErrT[M, Set[PathSegment]] =
        children(dir).leftMap(toFSError)

      val result: FileSystemErrT[M, ?] ~> Backend =
        Hoist[FileSystemErrT].hoist[M, PhaseResultT[Configured, ?]](
          λ[Configured ~> PhaseResultT[Configured, ?]](_.liftM[PhaseResultT])
            compose λ[M ~> Configured](_.liftM[ConfiguredT]))

      result(segments)
    }

    def fileExists(file: AFile): Configured[Boolean] = {
      val dir: ADir = pathy.Path.fileParent(file)

      val res: M[Boolean] = for {
        back <- children(dir).fold(_ => false, _.contains(pathy.Path.fileName(file).right))
      } yield back

      res.liftM[ConfiguredT]
    }
  }

  object ReadFileModule extends ReadFileModule {
    import ReadFile._

    def open(file: AFile, offset: Natural, limit: Option[Positive]): Backend[ReadHandle] = ???
    def read(h: ReadHandle): Backend[Vector[Data]] = ???
    def close(h: ReadHandle): Configured[Unit] = ???
  }

  object WriteFileModule extends WriteFileModule {
    import WriteFile._

    private val QueueLimit = 100

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
          jvs = dequeueStreamT(queue)(_.isEmpty).map(_.map(JValue.fromData))

          precog <- cake[M]

          ingestion = for {
            _ <- precog.ingest(path, jvs.trans(λ[Task ~> Future](_.unsafeToFuture))).toTask
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

      if (chunk.isEmpty) {
        Vector.empty[FileSystemError].point[Configured]
      } else {
        val t = for {
          pair <- Task.delay(Option(map.get(h)).get)
          (queue, _) = pair
          _ <- queue.enqueue1(chunk)
        } yield Vector.empty[FileSystemError]

        t.liftM[MT].liftM[ConfiguredT]
      }
    }

    def close(h: WriteHandle): Configured[Unit] = {
      val t = for {
        // yolo we crash because quasar
        pair <- Task.delay(Option(map.get(h)).get).liftM[MT]
        (queue, signal) = pair
        _ <- Task.delay(log.debug(s"close $h")).liftM[MT]
        // ask queue to stop
        _ <- queue.enqueue1(Vector.empty).liftM[MT]
        // wait until queue actually stops; task async completes when signal completes
        _ <- signal.discrete.takeWhile(!_).run.liftM[MT]
        precog <- cake[M]
        _ <- Task.delay(precog.stopPath(fileToPath(h.file))).liftM[MT]
      } yield ()

      t.liftM[ConfiguredT]
    }
  }

  // move and delete assume nihdb semantics
  object ManageFileModule extends ManageFileModule {
    import java.io.File

    import ManageFile._

    def move(scenario: MoveScenario, semantics: MoveSemantics): Backend[Unit] = {
      def inner(fpath: APath, dpath: APath): M[Unit] = for {
        precog <- cake[M]

        from = new File(precog.Config.dataDir, pathy.Path.posixCodec.printPath(fpath))
        dest = new File(precog.Config.dataDir, pathy.Path.posixCodec.printPath(dpath))

        _ <- Task.delay(from.renameTo(dest)).liftM[MT]
      } yield ()

      inner(scenario.src, scenario.dst).liftB
    }

    def delete(path: APath): Backend[Unit] = {
      val t = for {
        precog <- cake[M]
        target = new File(precog.Config.dataDir, pathy.Path.posixCodec.printPath(path))

        _ <- Task.delay(IOUtils.recursiveDelete(target).unsafePerformIO()).liftM[MT]
      } yield ()

      t.liftB
      //().point[Backend]
    }

    def tempFile(near: APath): Backend[AFile] = {
      for {
        seed <- Task.delay(Random.nextLong.toString).liftM[MT].liftB

        precog <- cake[M].liftB
        newDir <- Task.delay(new File(precog.Config.dataDir, pathy.Path.posixCodec.printPath(parentDir(near).get))).liftM[MT].liftB
        _ <- Task.delay(newDir.mkdirs()).liftM[MT].liftB

        // yolo
        target = parentDir(near).get </> file(seed)

        h <- WriteFileModule.open(target)
        _ <- WriteFileModule.write(h, Vector(Data.Obj())).liftM[PhaseResultT].liftM[FileSystemErrT]
        _ <- WriteFileModule.close(h).liftM[PhaseResultT].liftM[FileSystemErrT]
      } yield target
    }
  }
}

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

package quasar.physical.sparkcore.fs

import slamdata.Predef._
import quasar._
import quasar.connector.BackendModule
import quasar.contrib.pathy._
import quasar.common._
import quasar.fp._, free._
import quasar.fs._, FileSystemError._, PathError._
import quasar.fs.mount._, BackendDef._
import quasar.effect._
import quasar.qscript.{Read => _, _}

import scala.Predef.implicitly

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import pathy.Path._
import matryoshka._
import matryoshka.implicits._
import scalaz.{Failure => _, _}, Scalaz._
import scalaz.concurrent.Task

trait SparkCoreBackendModule extends BackendModule {

  // conntector specificc
  type Eff[A]
  def toLowerLevel[S[_]](sc: SparkContext)(implicit
    S0: Task :<: S, S1: PhysErr :<: S
  ): Task[M ~> Free[S, ?]]
  def generateSC: Config => DefErrT[Task, SparkContext]
  def rebaseAFile(f: AFile): Configured[AFile]
  def stripPrefixAFile(f: AFile): Configured[AFile]
  def rebaseADir(f: ADir): Configured[ADir]
  def stripPrefixADir(f: ADir): Configured[ADir]
  def ReadSparkContextInj: Inject[Read[SparkContext, ?], Eff]
  def RFKeyValueStoreInj: Inject[KeyValueStore[ReadFile.ReadHandle, SparkCursor, ?], Eff]
  def MonotonicSeqInj: Inject[MonotonicSeq, Eff]
  def TaskInj: Inject[Task, Eff]
  def SparkConnectorDetailsInj: Inject[SparkConnectorDetails, Eff]
  def QFKeyValueStoreInj: Inject[KeyValueStore[QueryFile.ResultHandle, queryfile.RddState, ?], Eff]

  // common for all spark based connecotrs

  type M[A] = Free[Eff, A]
  type QS[T[_[_]]] = QScriptCore[T, ?] :\: EquiJoin[T, ?] :/: Const[ShiftedRead[AFile], ?]
  type Repr = RDD[Data]

  private final implicit def _ReadSparkContextInj: Inject[Read[SparkContext, ?], Eff] =
    ReadSparkContextInj
  private final implicit def _RFKeyValueStoreInj: Inject[KeyValueStore[ReadFile.ReadHandle, SparkCursor, ?], Eff] =
    RFKeyValueStoreInj
  private final implicit def _MonotonicSeqInj: Inject[MonotonicSeq, Eff] =
    MonotonicSeqInj
  private final implicit def _TaskInj: Inject[Task, Eff] =
    TaskInj
  private final implicit def _SparkConnectorDetailsInj: Inject[SparkConnectorDetails, Eff] =
    SparkConnectorDetailsInj
  private final implicit def _QFKeyValueStoreInj: Inject[KeyValueStore[QueryFile.ResultHandle, queryfile.RddState, ?], Eff] =
    QFKeyValueStoreInj

  def detailsOps: SparkConnectorDetails.Ops[Eff] = SparkConnectorDetails.Ops[Eff]
  def readScOps: Read.Ops[SparkContext, Eff] = Read.Ops[SparkContext, Eff]
  def msOps: MonotonicSeq.Ops[Eff] = MonotonicSeq.Ops[Eff]
  def qfKvsOps: KeyValueStore.Ops[QueryFile.ResultHandle, queryfile.RddState, Eff] =
    KeyValueStore.Ops[QueryFile.ResultHandle, queryfile.RddState, Eff]

  def FunctorQSM[T[_[_]]] = Functor[QSM[T, ?]]
  def DelayRenderTreeQSM[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] =
    implicitly[Delay[RenderTree, QSM[T, ?]]]
  def ExtractPathQSM[T[_[_]]: RecursiveT] = ExtractPath[QSM[T, ?], APath]
  def QSCoreInject[T[_[_]]] = implicitly[QScriptCore[T, ?] :<: QSM[T, ?]]
  def MonadM = Monad[M]
  def UnirewriteT[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = implicitly[Unirewrite[T, QS[T]]]
  def UnicoalesceCap[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = Unicoalesce.Capture[T, QS[T]]

  type LowerLevel[A] = Coproduct[Task, PhysErr, A]
  def lowerToTask: LowerLevel ~> Task = λ[LowerLevel ~> Task](_.fold(
    injectNT[Task, Task],
    Failure.mapError[PhysicalError, Exception](_.cause) andThen Failure.toCatchable[Task, Exception]
  ))

  def toTask(sc: SparkContext): Task[M ~> Task] = toLowerLevel[LowerLevel](sc).map(_ andThen foldMapNT(lowerToTask))

  def compile(cfg: Config): DefErrT[Task, (M ~> Task, Task[Unit])] = for {
    sc <- generateSC(cfg)
    tt <- toTask(sc).liftM[DefErrT]
  } yield (tt, Task.delay(sc.stop()))

  private def stripErr(input: FileSystemError): Configured[FileSystemError] = input match {
    case UnknownReadHandle(h) =>
      stripPrefixAFile(h.file).map(f => (UnknownReadHandle(ReadFile.ReadHandle(f, h.id)):FileSystemError))
    case UnknownWriteHandle(h) =>
      stripPrefixAFile(h.file).map(f => (UnknownWriteHandle(WriteFile.WriteHandle(f, h.id)):FileSystemError))
    case PathErr(perr) =>
      refineType(errorPath.get(perr)).fold(
        dir  => stripPrefixADir(dir).map(d => (PathErr(errorPath.set(d)(perr)):FileSystemError)),
        file => stripPrefixAFile(file).map(f => (PathErr(errorPath.set(f)(perr)):FileSystemError))
      )
    case o       => o.point[Configured]
  }

  implicit class BackendRebase[A](b: Backend[A]) {
    def stripError: Backend[A] = 
      EitherT(b.run >>= (i => i.bitraverse(stripErr, _.point[Configured]).liftM[PhaseResultT]))
  }

  def rddFrom: AFile => Configured[RDD[Data]] =
    (file: AFile) => (rebaseAFile(file) >>= (f => detailsOps.rddFrom(f).liftM[ConfiguredT]))
  def first: RDD[Data] => M[Data] = (rdd: RDD[Data]) => lift(Task.delay {rdd.first}).into[Eff]

  def plan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](cp: T[QSM[T, ?]]): Backend[Repr] = for {
    config <- Kleisli.ask[M, Config].liftB
    repr   <- includeError((readScOps.ask >>= { sc =>
      val total = implicitly[Planner[QSM[T, ?], Eff]]
      cp.cataM(total.plan(f => rddFrom(f).run.apply(config), first)).eval(sc).run.map(_.leftMap(pe => qscriptPlanningFailed(pe)))
    }).liftB)
  } yield repr

  object SparkReadFileModule extends ReadFileModule {
    import ReadFile._
    import quasar.fp.numeric.{Natural, Positive}

    def open(file: AFile, offset: Natural, limit: Option[Positive]): Backend[ReadHandle] = for {
      f <- rebaseAFile(file).liftB.stripError
      h <- includeError(readfile.open[Eff](f, offset, limit).liftB)
    } yield ReadHandle(file, h.id)
    
    def read(h: ReadHandle): Backend[Vector[Data]] =
      rebaseAFile(h.file).liftB >>= (f => includeError(readfile.read[Eff](ReadHandle(f, h.id)).liftB).stripError)

    def close(h: ReadHandle): Configured[Unit] =
      rebaseAFile(h.file) >>= (f => readfile.close[Eff](ReadHandle(f, h.id)).liftM[ConfiguredT])
  }
  def ReadFileModule = SparkReadFileModule

  object SparkQueryFileModule extends QueryFileModule {
    import QueryFile._
    import queryfile.RddState

    def executePlan(rdd: RDD[Data], out: AFile): Backend[AFile] = rebaseAFile(out).liftB >>= { o =>
      val execute =  detailsOps.storeData(rdd, o).as(out)
      val log     = PhaseResult.detail("RDD", rdd.toDebugString)
      execute.withLog(log)
    }

    def evaluatePlan(rdd: Repr): Backend[ResultHandle] = (for {
      h <- msOps.next.map(ResultHandle(_))
      _ <- qfKvsOps.put(h, RddState(rdd.zipWithIndex.persist.some, 0))
    } yield h).withLog(PhaseResult.detail("RDD", rdd.toDebugString))

    def more(h: ResultHandle): Backend[Vector[Data]] = includeError(queryfile.more[Eff](h).liftB)

    def close(h: ResultHandle): Configured[Unit] = queryfile.close[Eff](h).liftM[ConfiguredT]

    def explain(rdd: RDD[Data]): Backend[String] =
      rdd.toDebugString.point[Backend]

    def listContents(dir: ADir): Backend[Set[PathSegment]] =
      (rebaseADir(dir).liftB >>= (d => includeError(detailsOps.listContents(d).run.liftB))).stripError

    def fileExists(file: AFile): Configured[Boolean] =
      rebaseAFile(file) >>= (f => detailsOps.fileExists(f).liftM[ConfiguredT])
  }

  def QueryFileModule: QueryFileModule = SparkQueryFileModule

  abstract class SparkCoreWriteFileModule extends WriteFileModule {
    import WriteFile._

    def rebasedOpen(file: AFile): Backend[WriteHandle]
    def rebasedWrite(h: WriteHandle, chunk: Vector[Data]): Configured[Vector[FileSystemError]]
    def rebasedClose(h: WriteHandle): Configured[Unit]

    def open(file: AFile): Backend[WriteHandle] = for {
       f <- rebaseAFile(file).liftB
      h <- rebasedOpen(f)
    } yield WriteHandle(file, h.id)

    def write(h: WriteHandle, chunk: Vector[Data]): Configured[Vector[FileSystemError]] = 
      rebaseAFile(h.file) >>= (f => rebasedWrite(WriteHandle(f, h.id), chunk)) >>= (_.traverse(stripErr))

    def close(h: WriteHandle): Configured[Unit] =
      rebaseAFile(h.file) >>= (f => rebasedClose(WriteHandle(f, h.id)))
  }

  abstract class SparkCoreManageFileModule extends ManageFileModule {
    import ManageFile._, ManageFile.MoveScenario._
    import quasar.fs.impl.ensureMoveSemantics

    def moveFile(src: AFile, dst: AFile): M[Unit]
    def moveDir(src: ADir, dst: ADir): M[Unit]
    def doesPathExist: APath => M[Boolean]
    def deleteFile(f: AFile): Backend[Unit]
    def deleteDir(d: ADir): Backend[Unit]
    def tempFileNearFile(f: AFile): Backend[AFile]
    def tempFileNearDir(d: ADir): Backend[AFile]

    def move(scenario: MoveScenario, semantics: MoveSemantics): Backend[Unit] = ((scenario, semantics) match {
      case (FileToFile(srcFile, destFile), semantics) => for {
        sf <- rebaseAFile(srcFile).liftB
        df <- rebaseAFile(destFile).liftB
        _  <- includeError(((ensureMoveSemantics(sf, df, doesPathExist, semantics).toLeft(()) *>
          moveFile(sf, df).liftM[FileSystemErrT]).run).liftB)
      } yield ()

      case (DirToDir(srcDir, destDir), semantics) => for {
        sd <- rebaseADir(srcDir).liftB
        dd <- rebaseADir(destDir).liftB
        _  <- includeError(((ensureMoveSemantics(sd, dd, doesPathExist, semantics).toLeft(()) *>
          moveDir(sd, dd).liftM[FileSystemErrT]).run).liftB)
      } yield ()
        
    }).stripError

    def delete(path: APath): Backend[Unit] =
      refineType(path).fold(
        dir  => rebaseADir(dir).liftB   >>= (deleteDir _),
        file => rebaseAFile(file).liftB >>= (deleteFile _) 
      ).stripError

    def tempFile(near: APath): Backend[AFile] = {
      refineType(near).fold(
        dir  => rebaseADir(dir).liftB   >>= (tempFileNearDir _)  >>= (f => stripPrefixAFile(f).liftB),
        file => rebaseAFile(file).liftB >>= (tempFileNearFile _) >>= (f => stripPrefixAFile(f).liftB)
      ).stripError
    }
  }
}

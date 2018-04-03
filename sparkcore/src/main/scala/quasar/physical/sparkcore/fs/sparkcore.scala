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
import quasar.concurrent.Pools
import quasar.connector._
import quasar.contrib.scalaz._
import quasar.contrib.scalaz.concurrent._
import quasar.contrib.pathy._
import quasar.common._
import quasar.fp._, free._, ski.κ
import quasar.fs._, FileSystemError._
import quasar.fs.mount._, BackendDef._
import quasar.effect._
import quasar.qscript.{Read => _, _}
import quasar.qscript.analysis._
import quasar.qscript.rewrites.{Optimize, Unicoalesce, Unirewrite}

import java.lang.{Runnable, Runtime, Thread}
import scala.Predef.implicitly
import java.net.URLDecoder
import java.util.concurrent.{Executors, ExecutorService, ThreadFactory}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.ExecutionContext
import scala.{math, StringContext}

import org.apache.spark.rdd.RDD
import org.apache.spark._
import pathy.Path._
import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import scalaz.{Failure => _, _}, Scalaz._
import scalaz.concurrent.{Strategy, Task}

final case class SparkCursor(rdd: Option[RDD[(Data, Long)]], pointer: Int)

trait SparkCore extends BackendModule with DefaultAnalyzeModule {

  // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
  import EitherT.eitherTMonad

  import SparkCore.shiftReShift

  // conntector specific
  type Eff[A]
  def toLowerLevel[S[_]](sc: SparkContext, config: Config)(implicit
    S0: Task :<: S, S1: PhysErr :<: S
  ): Task[M ~> Free[S, ?]]
  def generateSC: (APath, Config) => DefErrT[Task, SparkContext]
  def getSparkConf: Config => SparkConf
  def ReadSparkContextInj: Inject[Read[SparkContext, ?], Eff]
  def RFKeyValueStoreInj: Inject[KeyValueStore[ReadFile.ReadHandle, SparkCursor, ?], Eff]
  def MonotonicSeqInj: Inject[MonotonicSeq, Eff]
  def TaskInj: Inject[Task, Eff]
  def SparkConnectorDetailsInj: Inject[SparkConnectorDetails, Eff]
  def QFKeyValueStoreInj: Inject[KeyValueStore[QueryFile.ResultHandle, SparkCursor, ?], Eff]

  // common for all spark based connecotrs
  type M[A] = Free[Eff, A]
  type QS[T[_[_]]] = SparkCoreQScriptCP[T]
  type Repr = RDD[Data]

  implicit def qScriptToQScriptTotal[T[_[_]]]: Injectable.Aux[QSM[T, ?], QScriptTotal[T, ?]] =
    quasar.physical.sparkcore.fs.qScriptToQScriptTotal[T]

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
  private final implicit def _QFKeyValueStoreInj: Inject[KeyValueStore[QueryFile.ResultHandle, SparkCursor, ?], Eff] =
    QFKeyValueStoreInj

  def optimize[T[_[_]]: BirecursiveT: EqualT: ShowT]
      : QSM[T, T[QSM[T, ?]]] => QSM[T, T[QSM[T, ?]]] = {
      val O = new Optimize[T]
      O.optimize(fp.reflNT[QSM[T, ?]])
  }

  def detailsOps: SparkConnectorDetails.Ops[Eff] = SparkConnectorDetails.Ops[Eff]
  def readScOps: Read.Ops[SparkContext, Eff] = Read.Ops[SparkContext, Eff]
  def msOps: MonotonicSeq.Ops[Eff] = MonotonicSeq.Ops[Eff]
  def qfKvsOps: KeyValueStore.Ops[QueryFile.ResultHandle, SparkCursor, Eff] =
    KeyValueStore.Ops[QueryFile.ResultHandle, SparkCursor, Eff]

  import Cost._
  import Cardinality._

  def CardinalityQSM: Cardinality[QSM[Fix, ?]] = Cardinality[QSM[Fix, ?]]
  def CostQSM: Cost[QSM[Fix, ?]] = Cost[QSM[Fix, ?]]
  def FunctorQSM[T[_[_]]] = Functor[QSM[T, ?]]
  def TraverseQSM[T[_[_]]] = Traverse[QSM[T, ?]]
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

  def toTask(sc: SparkContext, config: Config): Task[M ~> Task] =
    toLowerLevel[LowerLevel](sc, config).map(_ andThen foldMapNT(lowerToTask)).map(_.andThen(shiftReShift))

  def sparkCoreJar: DefErrT[Task, APath] = {

    val absStrToAPath: String => Option[APath] = (str: String) => posixCodec.parsePath[Option[APath]](
      κ(None), unsafeSandboxAbs(_).some, κ(None), unsafeSandboxAbs(_).some)(str)

    val pathFromEnvVariable: Task[Option[APath]] = Task.delay {
      scala.sys.env.get("SPARKCORE_JAR_PATH").map(absStrToAPath).join
    }

    val pathFromProjectRoot: OptionT[Task, APath] = for {
      pluginFolderPath <- OptionT(Task.delay {
        val encodedPathStr = this.getClass().getProtectionDomain.getCodeSource.getLocation.toURI.getPath
        absStrToAPath(URLDecoder.decode(encodedPathStr, "UTF-8"))
      })
      rootPath <- OptionT(parentDir(pluginFolderPath).map(parentDir(_)).join.point[Task])
    } yield rootPath </> file("sparkcore.jar")

    val finalPath: OptionT[Task, APath] = OptionT(pathFromEnvVariable >>= (_.cata(
      path => path.some.point[Task], pathFromProjectRoot.run
    )))

    finalPath.toRight(NonEmptyList("Could not fetch sparkcore.jar").left[EnvironmentError])
  }

  def initSC: Config => DefErrT[Task, SparkContext] = (config: Config) => EitherT(Task.delay {
    new SparkContext(getSparkConf(config)).right[DefinitionError]
  }.handleWith {
    case ex : SparkException if ex.getMessage.contains("SPARK-2243") =>
      NonEmptyList("You can not mount second Spark based connector... " +
        "Please unmount existing one first.").left[EnvironmentError].left[SparkContext].point[Task]
  }).mapT(shiftReShift(_))

  def compile(cfg: Config): DefErrT[Task, (M ~> Task, Task[Unit])] = for {
    jar <- sparkCoreJar
    sc  <- generateSC(jar, cfg)
    tt  <- toTask(sc, cfg).liftM[DefErrT]
  } yield (tt, Task.delay(sc.stop()))

  def rddFrom: AFile => Configured[RDD[Data]] =
    (f: AFile) => detailsOps.rddFrom(f).liftM[ConfiguredT]
  def first: RDD[Data] => M[Data] = (rdd: RDD[Data]) => lift(Task.delay {rdd.first}).into[Eff]

  def plan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](cp: T[QSM[T, ?]]): Backend[Repr] = for {
    config <- Kleisli.ask[M, Config].liftB
    repr   <- (readScOps.ask >>= { sc =>
      val total = implicitly[Planner[QSM[T, ?], Eff]]
      cp.cataM(total.plan(f => rddFrom(f).run.apply(config), first)).eval(sc).run.map(_.leftMap(pe => qscriptPlanningFailed(pe)))
    }).liftB.unattempt
  } yield repr

  object SparkReadFileModule extends ReadFileModule {
    import ReadFile._
    import quasar.fp.numeric.{Natural, Positive}

    def open(f: AFile, offset: Natural, limit: Option[Positive]): Backend[ReadHandle] = for {
      h <- readfile.open[Eff](f, offset, limit).liftB.unattempt
    } yield ReadHandle(f, h.id)

    def read(h: ReadHandle): Backend[Vector[Data]] =
      readfile.read[Eff](h).liftB.unattempt

    def close(h: ReadHandle): Configured[Unit] =
      readfile.close[Eff](h).liftM[ConfiguredT]
  }
  def ReadFileModule = SparkReadFileModule

  object SparkQueryFileModule extends QueryFileModule {
    import QueryFile._

    def executePlan(rdd: RDD[Data], out: AFile): Backend[Unit] = {
      val execute =  detailsOps.storeData(rdd, out).liftB
      val log     = Vector(PhaseResult.detail("RDD", rdd.toDebugString))
      execute :++> log
    }

    def evaluatePlan(rdd: Repr): Backend[ResultHandle] = (for {
      h <- msOps.next.map(ResultHandle(_))
      _ <- qfKvsOps.put(h, SparkCursor(rdd.zipWithIndex.persist.some, 0))
    } yield h).liftB :++> Vector(PhaseResult.detail("RDD", rdd.toDebugString))

    def more(h: ResultHandle): Backend[Vector[Data]] = (for {
    step <- detailsOps.readChunkSize
    res  <- (qfKvsOps.get(h).toRight(unknownResultHandle(h)).flatMap {
      case SparkCursor(None, _) =>
        Vector.empty[Data].pure[EitherT[Free[Eff, ?], FileSystemError, ?]]
      case SparkCursor(Some(rdd), p) =>
        for {
          collected <- lift(Task.delay {
            rdd
              .filter(d => d._2 >= p && d._2 < (p + step))
              .map(_._1).collect.toVector
          }).into[Eff].liftM[FileSystemErrT]
          rddState <- lift(Task.delay {
            if(collected.isEmpty) {
              ignore(rdd.unpersist())
              SparkCursor(None, 0)
            } else SparkCursor(Some(rdd), p + step)
          }).into[Eff].liftM[FileSystemErrT]
          _ <- qfKvsOps.put(h, rddState).liftM[FileSystemErrT]
        } yield collected
    }).run
  } yield res).liftB.unattempt

    def close(h: ResultHandle): Configured[Unit] = qfKvsOps.delete(h).liftM[ConfiguredT]

    def explain(rdd: RDD[Data]): Backend[String] =
      rdd.toDebugString.point[Backend]

    def listContents(dir: ADir): Backend[Set[PathSegment]] =
      detailsOps.listContents(dir).run.liftB.unattempt

    def fileExists(file: AFile): Configured[Boolean] =
      detailsOps.fileExists(file).liftM[ConfiguredT]
  }

  def QueryFileModule: QueryFileModule = SparkQueryFileModule

  abstract class SparkCoreManageFileModule extends ManageFileModule {
    import ManageFile._, ManageFile.PathPair._
    import quasar.fs.impl.ensureMoveSemantics

    def moveFile(src: AFile, dst: AFile): M[Unit]
    def moveDir(src: ADir, dst: ADir): M[Unit]
    def doesPathExist: APath => M[Boolean]

    def move(scenario: PathPair, semantics: MoveSemantics): Backend[Unit] = ((scenario, semantics) match {
      case (FileToFile(sf, df), semantics) => for {
        _  <- (((ensureMoveSemantics(sf, df, doesPathExist, semantics).toLeft(()) *>
          moveFile(sf, df).liftM[FileSystemErrT]).run).liftB).unattempt
      } yield ()

      case (DirToDir(sd, dd), semantics) => for {
        _  <- (((ensureMoveSemantics(sd, dd, doesPathExist, semantics).toLeft(()) *>
          moveDir(sd, dd).liftM[FileSystemErrT]).run).liftB).unattempt
      } yield ()
    })

    def copy(pair: PathPair): Backend[Unit] =
      unsupportedOperation("Spark connector does not currently support copy operation").left[Unit].point[M].liftB.unattempt

    val defaultPrefix = TempFilePrefix("quasar-")

    def tempFile(near: APath, prefix: Option[TempFilePrefix]): Backend[AFile] = lift(Task.delay {
      val random = scala.util.Random.nextInt().toString
      TmpFile.tmpFile0(near, prefix.getOrElse(defaultPrefix), s"$random.tmp").right[FileSystemError]
    }
    ).into[Eff].liftB.unattempt
  }
}

object SparkCore {

  def shiftReShift: Task ~> Task = λ[Task ~> Task] { ta =>
    for {
      _ <- shift(SparkCore.CPUStrategy)
      a <- ta
      _ <- shift(Pools.CPUStrategy)
    } yield a
  }

  // we build a special pool for spark to avoid a) starvation, and b) thread local pollution
  private[sparkcore] val CPUExecutor: ExecutorService = {
    val numThreads = math.max(Runtime.getRuntime.availableProcessors, 4)
    Executors.newFixedThreadPool(numThreads, new ThreadFactory {
      private val counter = new AtomicInteger(0)

      def newThread(r: Runnable): Thread = {
        val t = new Thread(r)

        /*
         * The classloader stuff is a bit tricky here.  Basically, Spark and Hadoop
         * are circumventing the classloader hierarchy by reading the context classloader.
         * This is rather annoying, but there's not a lot we can do about it.  The
         * context classloader is stored in a thread local, but since we're running on
         * a pool, we don't really have tight control over which thread we're on.  So
         * we have to constantly and aggressively re-set the context classloader.  This
         * is also done prior to generating the SparkContext in the derived connectors.
         */
        t.setContextClassLoader(getClass.getClassLoader)

        t.setName(s"sparkcore-cpu-thread-${counter.getAndIncrement}")
        t
      }
    })
  }

  private[sparkcore] val CPUExecutionContext: ExecutionContext =
    ExecutionContext.fromExecutorService(CPUExecutor)

  private[sparkcore] val CPUStrategy: Strategy = Strategy.Executor(CPUExecutor)
}

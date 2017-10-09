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

package quasar
package connector

import slamdata.Predef._
import quasar.Data
import quasar.common._
import quasar.contrib.pathy._
import quasar.contrib.matryoshka._
import quasar.contrib.scalaz._, eitherT._
import quasar.fp._
import quasar.fp.free._
import quasar.fp.numeric.{Natural, Positive}
import quasar.frontend.logicalplan.LogicalPlan
import quasar.fs._
import quasar.fs.mount._
import quasar.qscript._

import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

trait BackendModule {
  import BackendDef.{DefErrT, DefinitionResult}
  import BackendModule._

  type QSM[T[_[_]], A] = QS[T]#M[A]

  type ConfiguredT[F[_], A] = Kleisli[F, Config, A]
  type Configured[A]        = ConfiguredT[M, A]
  type BackendT[F[_], A]    = FileSystemErrT[PhaseResultT[ConfiguredT[F, ?], ?], A]
  type Backend[A]           = BackendT[M, A]

  private final implicit def _FunctorQSM[T[_[_]]] = FunctorQSM[T]
  private final implicit def _DelayRenderTreeQSM[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]: Delay[RenderTree, QSM[T, ?]] = DelayRenderTreeQSM
  private final implicit def _ExtractPathQSM[T[_[_]]: RecursiveT]: ExtractPath[QSM[T, ?], APath] = ExtractPathQSM
  private final implicit def _QSCoreInject[T[_[_]]] = QSCoreInject[T]
  private final implicit def _MonadM = MonadM
  private final implicit def _UnirewriteT[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = UnirewriteT[T]
  private final implicit def _UnicoalesceCap[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = UnicoalesceCap[T]

  implicit class LiftBackend[A](m: M[A]) {
    val liftB: Backend[A] = m.liftM[ConfiguredT].liftM[PhaseResultT].liftM[FileSystemErrT]
  }

  implicit class LiftBackendConfigured[A](c: Configured[A]) {
    val liftB: Backend[A] = c.liftM[PhaseResultT].liftM[FileSystemErrT]
  }

  final val definition: BackendDef[Task] =
    BackendDef fromPF {
      case (Type, uri) =>
        (parseConfig(uri) >>= interpreter) map { case (f, c) => DefinitionResult(f, c) }
    }

  def interpreter(cfg: Config): DefErrT[Task, (BackendEffect ~> Task, Task[Unit])] =
    compile(cfg) map {
      case (runM, close) =>
        val runCfg = λ[Configured ~> M](_.run(cfg))
        val runFs: BackendEffect ~> Configured = analyzeInterpreter :+: fsInterpreter
        (runM compose runCfg compose runFs, close)
    }


  private final def analyzeInterpreter: Analyze ~> Configured = {
    val lc: DiscoverPath.ListContents[Backend] =
      QueryFileModule.listContents(_)

    λ[Analyze ~> Configured]({
      case Analyze.QueryCost(lp) => 10.right[FileSystemError].point[Configured]
    })
  }

  private final def fsInterpreter: FileSystem ~> Configured = {
    val qfInter: QueryFile ~> Configured = λ[QueryFile ~> Configured] {
      case QueryFile.ExecutePlan(lp, out) =>
        lpToRepr(lp).flatMap(p => QueryFileModule.executePlan(p.repr, out)).run.run

      case QueryFile.EvaluatePlan(lp) =>
        lpToRepr(lp).flatMap(p => QueryFileModule.evaluatePlan(p.repr)).run.run

      case QueryFile.More(h) => QueryFileModule.more(h).run.value
      case QueryFile.Close(h) => QueryFileModule.close(h)

      case QueryFile.Explain(lp) =>
        (for {
          pp <- lpToRepr(lp)
          explanation <- QueryFileModule.explain(pp.repr)
        } yield ExecutionPlan(Type, explanation, pp.paths)).run.run

      case QueryFile.ListContents(dir) => QueryFileModule.listContents(dir).run.value
      case QueryFile.FileExists(file) => QueryFileModule.fileExists(file)
    }

    val rfInter: ReadFile ~> Configured = λ[ReadFile ~> Configured] {
      case ReadFile.Open(file, offset, limit) =>
        ReadFileModule.open(file, offset, limit).run.value

      case ReadFile.Read(h) => ReadFileModule.read(h).run.value
      case ReadFile.Close(h) => ReadFileModule.close(h)
    }

    val wfInter: WriteFile ~> Configured = λ[WriteFile ~> Configured] {
      case WriteFile.Open(file) => WriteFileModule.open(file).run.value
      case WriteFile.Write(h, chunk) => WriteFileModule.write(h, chunk)
      case WriteFile.Close(h) => WriteFileModule.close(h)
    }

    val mfInter: ManageFile ~> Configured = λ[ManageFile ~> Configured] {
      case ManageFile.Move(pathPair, semantics) =>
        ManageFileModule.move(pathPair, semantics).run.value
      case ManageFile.Copy(pathPair) => ManageFileModule.copy(pathPair).run.value
      case ManageFile.Delete(path) => ManageFileModule.delete(path).run.value
      case ManageFile.TempFile(near) => ManageFileModule.tempFile(near).run.value
    }

    qfInter :+: rfInter :+: wfInter :+: mfInter
  }

  final def lpToQScript
    [T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      M[_]: Monad: MonadFsErr: PhaseResultTell]
    (lp: T[LogicalPlan], lc: DiscoverPath.ListContents[M])
      : M[T[QSM[T, ?]]] = {

    type QSR[A] = QScriptRead[T, A]

    val R = new Rewrite[T]

    for {
      qs <- QueryFile.convertToQScriptRead[T, M, QSR](lc)(lp)
      shifted <- Unirewrite[T, QS[T], M](R, lc).apply(qs)

      _ <- logPhase[M](PhaseResult.tree("QScript (ShiftRead)", shifted))

      optimized =
        shifted.transHylo(optimize[T], Unicoalesce.Capture[T, QS[T]].run)

      _ <- logPhase[M](PhaseResult.tree("QScript (Optimized)", optimized))
    } yield optimized
  }

  final def lpToRepr[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](
      lp: T[LogicalPlan]): Backend[PhysicalPlan[Repr]] = {

    val lc: DiscoverPath.ListContents[Backend] =
      QueryFileModule.listContents(_)

    for {
      optimized <- lpToQScript[T, Backend](lp, lc)
      main <- plan(optimized)
      inputs = optimized.cata(ExtractPath[QSM[T, ?], APath].extractPath[DList])
    } yield PhysicalPlan(main, ISet.fromFoldable(inputs))
  }

  final def config[F[_]](implicit C: MonadReader_[F, Config]): F[Config] =
    C.ask

  // everything abstract below this line

  type QS[T[_[_]]] <: CoM

  implicit def qScriptToQScriptTotal[T[_[_]]]: Injectable.Aux[QSM[T, ?], QScriptTotal[T, ?]]

  type Repr
  type M[A]

  def FunctorQSM[T[_[_]]]: Functor[QSM[T, ?]]
  def DelayRenderTreeQSM[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]: Delay[RenderTree, QSM[T, ?]]
  def ExtractPathQSM[T[_[_]]: RecursiveT]: ExtractPath[QSM[T, ?], APath]
  def QSCoreInject[T[_[_]]]: QScriptCore[T, ?] :<: QSM[T, ?]
  def MonadM: Monad[M]
  def UnirewriteT[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]: Unirewrite[T, QS[T]]
  def UnicoalesceCap[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT]: Unicoalesce.Capture[T, QS[T]]

  def optimize[T[_[_]]: BirecursiveT: EqualT: ShowT]: QSM[T, T[QSM[T, ?]]] => QSM[T, T[QSM[T, ?]]]
  type Config
  def parseConfig(uri: ConnectionUri): DefErrT[Task, Config]

  def compile(cfg: Config): DefErrT[Task, (M ~> Task, Task[Unit])]

  val Type: FileSystemType

  def plan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](cp: T[QSM[T, ?]]): Backend[Repr]

  trait QueryFileModule {
    import QueryFile._

    def executePlan(repr: Repr, out: AFile): Backend[Unit]
    def evaluatePlan(repr: Repr): Backend[ResultHandle]
    def more(h: ResultHandle): Backend[Vector[Data]]
    def close(h: ResultHandle): Configured[Unit]
    def explain(repr: Repr): Backend[String]
    def listContents(dir: ADir): Backend[Set[PathSegment]]
    def fileExists(file: AFile): Configured[Boolean]
  }

  def QueryFileModule: QueryFileModule

  trait ReadFileModule {
    import ReadFile._

    def open(file: AFile, offset: Natural, limit: Option[Positive]): Backend[ReadHandle]
    def read(h: ReadHandle): Backend[Vector[Data]]
    def close(h: ReadHandle): Configured[Unit]
  }

  def ReadFileModule: ReadFileModule

  trait WriteFileModule {
    import WriteFile._

    def open(file: AFile): Backend[WriteHandle]
    def write(h: WriteHandle, chunk: Vector[Data]): Configured[Vector[FileSystemError]]
    def close(h: WriteHandle): Configured[Unit]
  }

  def WriteFileModule: WriteFileModule

  trait ManageFileModule {
    import ManageFile._

    def move(pair: PathPair, semantics: MoveSemantics): Backend[Unit]
    def copy(pair: PathPair): Backend[Unit]
    def delete(path: APath): Backend[Unit]
    def tempFile(near: APath): Backend[AFile]
  }

  def ManageFileModule: ManageFileModule

  trait AnalyzeModule {

    def queryCost(qs: Fix[QSM[Fix, ?]]): Backend[Int]
  }

  def AnalyzeModule: AnalyzeModule
}

object BackendModule {
  final def logPhase[M[_]: Monad: PhaseResultTell](pr: PhaseResult): M[Unit] =
    PhaseResultTell[M].tell(Vector(pr))
}

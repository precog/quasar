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
import quasar.{Data, RenderTree, RenderTreeT}
import quasar.common.PhaseResult
import quasar.connector.{BackendModule, DefaultAnalyzeModule}
import quasar.contrib.pathy._
import quasar.fp._
import quasar.contrib.iota._
import quasar.fp.numeric._
import quasar.fp.ski.ι
import quasar.fs._
import quasar.fs.mount.{BackendDef, ConnectionUri}
import quasar.qscript._
import quasar.qscript.analysis.{Cost, Cardinality}
import quasar.qscript.rewrites.{Unicoalesce, Unirewrite}

import argonaut._, Argonaut._
import matryoshka._
import matryoshka.data._
import scalaz.{Node => _, _}, Scalaz._
import scalaz.concurrent.Task
import scala.Predef.implicitly

object LightweightTester extends BackendModule with DefaultAnalyzeModule {

  object TestConnector extends SlamEngine {
    val Type: FileSystemType = FileSystemType("") // never referred to by its FileSystemType
    val lwc: LightweightConnector = LocalLightweight
  }

  type QS[T[_[_]]] = TestConnector.QS[T]

  implicit def qScriptToQScriptTotal[T[_[_]]]: Injectable[QSM[T, ?], QScriptTotal[T, ?]] =
    TestConnector.qScriptToQScriptTotal[T]

  type Repr = TestConnector.Repr
  type M[A] = Kleisli[Task, FileSystem ~> Task \/ (MimirCake.Cake, LightweightFileSystem, TestConnector.Config), A]

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

  def optimize[T[_[_]]: BirecursiveT: EqualT: ShowT]
      : QSM[T, T[QSM[T, ?]]] => QSM[T, T[QSM[T, ?]]] =
    TestConnector.optimize[T]

  sealed abstract class Config
  final case class InsertConfig() extends Config
  final case class TestConfig(mimirDataDir: java.io.File) extends Config

  // lwc_local="{\"mimirDataDir\": \"/path/to/data/\"}"
  // lwc_local_insert="{}"
  def parseConfig(uri: ConnectionUri): BackendDef.DefErrT[Task, Config] = {

    case class Config0(mimirDataDir: Option[String])

    implicit val CodecConfig0 =
      casecodec1(Config0.apply, Config0.unapply)("mimirDataDir")

    val config0: BackendDef.DefErrT[Task, Config0] = EitherT.eitherT {
      Task.delay {
        uri.value.parse.map(_.as[Config0]) match {
          case Left(err) =>
            NonEmptyList(err).left[EnvironmentError].left[Config0]
          case Right(DecodeResult(Left((err, _)))) =>
            NonEmptyList(err).left[EnvironmentError].left[Config0]
          case Right(DecodeResult(Right(config))) =>
            config.right[BackendDef.DefinitionError]
        }
      }
    }

    config0.flatMap {
      case Config0(None) =>
        (InsertConfig(): Config).point[BackendDef.DefErrT[Task, ?]]

      case Config0(Some(mimirDataDir)) =>
        val dir = new java.io.File(mimirDataDir)

        if (!dir.isAbsolute)
          EitherT.leftT(
            NonEmptyList("Mimir cannot be mounted to a relative path").left.point[Task])
        else
          (TestConfig(dir): Config).point[BackendDef.DefErrT[Task, ?]]
    }
  }

  def compile(cfg: Config): BackendDef.DefErrT[Task, (M ~> Task, Task[Unit])] =
    cfg match {
      case InsertConfig() => EitherT.rightT {
        Local.runFs.map { fs =>
          (λ[M ~> Task](_.run(fs.left)), Task.now(()))
        }
      }

      case TestConfig(mimirDataDir) =>
        val conf = TestConnector.Config(mimirDataDir, ConnectionUri(""))
        TestConnector.compile(conf).map {
          case (trans, shutdown) =>
            val testM = λ[M ~> TestConnector.M](m => Kleisli {
              case (t1, t2) => m.run((t1, t2, conf).right)
            })
            (trans compose testM, shutdown)
        }
    }

  val Type = FileSystemType("lwc_local")

  def plan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](cp: T[QSM[T, ?]])
      : Backend[Repr] =
    backendError[Repr](
      TestConnector.plan(cp),
      "Local file system cannot plan.")

  object QueryFileModule extends QueryFileModule {
    import QueryFile.ResultHandle

    def executePlan(repr: Repr, out: AFile): Backend[Unit] =
      backendError[Unit](
        TestConnector.QueryFileModule.executePlan(repr, out),
        "Local file system cannot execute plans.")

    def evaluatePlan(repr: Repr): Backend[ResultHandle] =
      backendError[ResultHandle](
        TestConnector.QueryFileModule.evaluatePlan(repr),
        "Local file system cannot evaluate plans.")

    def more(h: ResultHandle): Backend[Vector[Data]] =
      backendError[Vector[Data]](
        TestConnector.QueryFileModule.more(h),
        "Local file system cannot call `more` in QueryFileModule.")

    def close(h: ResultHandle): Configured[Unit] =
      configuredError[Unit](
        TestConnector.QueryFileModule.close(h),
        "Local file system cannot call `close` in QueryFileModule.")

    def explain(repr: Repr): Backend[String] =
      backendError[String](
        TestConnector.QueryFileModule.explain(repr),
        "Local file system cannot explain plans.")

    def listContents(dir: ADir): Backend[Set[PathSegment]] =
      backendB[Set[PathSegment], Set[Node], QueryFile](
        TestConnector.QueryFileModule.listContents(dir),
        QueryFile.ListContents(dir),
        _.map(_.segment))

    def fileExists(file: AFile): Configured[Boolean] =
      configured[Boolean, QueryFile](
        TestConnector.QueryFileModule.fileExists(file),
        QueryFile.FileExists(file))
  }

  object ReadFileModule extends ReadFileModule {
    import ReadFile.ReadHandle

    def open(file: AFile, offset: Natural, limit: Option[Positive]): Backend[ReadHandle] =
      backend[ReadHandle, ReadFile](
        TestConnector.ReadFileModule.open(file, offset, limit),
        ReadFile.Open(file, offset, limit))

    def read(h: ReadHandle): Backend[Vector[Data]] =
      backend[Vector[Data], ReadFile](
        TestConnector.ReadFileModule.read(h),
        ReadFile.Read(h))

    def close(h: ReadHandle): Configured[Unit] =
      configured[Unit, ReadFile](
        TestConnector.ReadFileModule.close(h),
        ReadFile.Close(h))
  }

  object WriteFileModule extends WriteFileModule {
    import WriteFile.WriteHandle

    def open(file: AFile): Backend[WriteHandle] =
      backend[WriteHandle, WriteFile](
        TestConnector.WriteFileModule.open(file),
        WriteFile.Open(file))

    def write(h: WriteHandle, chunk: Vector[Data]): Configured[Vector[FileSystemError]] =
      configured[Vector[FileSystemError], WriteFile](
        TestConnector.WriteFileModule.write(h, chunk),
        WriteFile.Write(h, chunk))

    def close(h: WriteHandle): Configured[Unit] =
      configured[Unit, WriteFile](
        TestConnector.WriteFileModule.close(h),
        WriteFile.Close(h))
  }

  object ManageFileModule extends ManageFileModule {
    import ManageFile.PathPair

    def move(scenario: PathPair, semantics: MoveSemantics): Backend[Unit] =
      backend[Unit, ManageFile](
        TestConnector.ManageFileModule.move(scenario, semantics),
        ManageFile.Move(scenario, semantics))

    def copy(pair: PathPair): Backend[Unit] =
      backend[Unit, ManageFile](
        TestConnector.ManageFileModule.copy(pair),
        ManageFile.Copy(pair))

    def delete(path: APath): Backend[Unit] =
      backend[Unit, ManageFile](
        TestConnector.ManageFileModule.delete(path),
        ManageFile.Delete(path))

    def tempFile(near: APath, prefix: Option[TempFilePrefix]): Backend[AFile] =
      backend[AFile, ManageFile](
        TestConnector.ManageFileModule.tempFile(near, prefix),
        ManageFile.TempFile(near, prefix))
  }

  ////////

  private def configured[A, FS[_]](
    runTest: TestConnector.Configured[A],
    fsComponent: FS[A])(
    implicit I: FS :<: FileSystem)
      : Configured[A] = {
    val result: M[A] = Kleisli.kleisli {
      case \/-((cake, fs, config)) =>
        runTest.run(config).apply((cake, fs))
      case -\/(local) =>
        val fs = PrismNT.inject[FS, FileSystem].apply(fsComponent)
        local.apply(fs)
    }
    result.liftM[ConfiguredT]
  }

  private def backendB[A, B, FS[_]](
    runTest: TestConnector.Backend[A],
    fsComponent: FS[FileSystemError \/ B],
    toA: B => A)(
    implicit I: FS :<: FileSystem)
      : Backend[A] = {
    val result: M[(Vector[PhaseResult], FileSystemError \/ A)] = Kleisli {
      case \/-((cake, fs, config)) =>
        runTest.run.run.run(config).apply((cake, fs))
      case -\/(local) =>
        val fs = PrismNT.inject[FS, FileSystem].apply(fsComponent)
        local.apply(fs).map(Vector() -> _.rightMap(toA))
    }
    EitherT.eitherT(WriterT.writerT(result.liftM[ConfiguredT]))
  }

  private def backend[A, FS[_]](
    runTest: TestConnector.Backend[A],
    fsComponent: FS[FileSystemError \/ A])(
    implicit I: FS :<: FileSystem)
      : Backend[A] =
    backendB[A, A, FS](runTest, fsComponent, ι)

  private def configuredError[A](runTest: TestConnector.Configured[A], errMsg: String) = {
    val result: M[A] = Kleisli {
      case \/-((cake, fs, config)) =>
        runTest.run(config).apply((cake, fs))
      case -\/(_) =>
        sys.error(errMsg)
    }
    result.liftM[ConfiguredT]
  }

  private def backendError[A](runTest: TestConnector.Backend[A], errMsg: String) = {
    val result: M[(Vector[PhaseResult], FileSystemError \/ A)] = Kleisli {
      case \/-((cake, fs, config)) =>
        runTest.run.run.run(config).apply((cake, fs))
      case -\/(_) =>
        sys.error(errMsg)
    }
    EitherT.eitherT(WriterT.writerT(result.liftM[ConfiguredT]))
  }
}

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
package physical.marklogic

import slamdata.Predef._
import quasar.common._
import quasar.connector._
import quasar.contrib.matryoshka._
import quasar.contrib.pathy._
import quasar.contrib.scalaz._
import quasar.contrib.scalaz.catchable._
import quasar.contrib.scalaz.eitherT._
import quasar.contrib.scalaz.writerT._
import quasar.effect._
import quasar.effect.uuid.UuidReader
import quasar.ejson.EJson
import quasar.fp._
import quasar.fp.numeric._
import quasar.fs._, FileSystemError._, PathError._
import quasar.fs.impl.{dataStreamRead, dataStreamClose}
import quasar.fs.mount._
import quasar.physical.marklogic.cts.Query
import quasar.physical.marklogic.fs._
import quasar.physical.marklogic.qscript._
import quasar.physical.marklogic.xcc._, Xcc.ops._
import quasar.physical.marklogic.xquery._
import quasar.physical.marklogic.xquery.syntax._
import quasar.qscript.{Read => QRead, _}
import quasar.qscript.analysis._

import scala.Predef.implicitly

import eu.timepit.refined.auto._
import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import scalaz.stream.Process

sealed class MarkLogic protected (readChunkSize: Positive, writeChunkSize: Positive)
    extends BackendModule
    with DefaultAnalyzeModule
    with ManagedQueryFile[XccDataStream]
    with ManagedWriteFile[AFile]
    with ManagedReadFile[XccDataStream]
    with ChrootedInterpreter {

  type QS[T[_[_]]] = MLQScriptCP[T]
  type Repr        = MainModule
  type Config      = MLBackendConfig
  type M[A]        = MLFS[A]
  type V[T[_[_]]]  = T[EJson]
  type Q[T[_[_]]]  = T[Query[V[T], ?]]

  val Type = FsType

  implicit def qScriptToQScriptTotal[T[_[_]]]: Injectable.Aux[QSM[T, ?], QScriptTotal[T, ?]] =
    ::\::[QScriptCore[T, ?]](::\::[ThetaJoin[T, ?]](::/::[T, Const[ShiftedRead[ADir], ?], Const[QRead[AFile], ?]]))

  // BackendModule
  def CardinalityQSM: Cardinality[QSM[Fix, ?]] = {
    import Cardinality._
    Cardinality[QSM[Fix, ?]]
  }
  def CostQSM: Cost[QSM[Fix, ?]] = {
    import Cost._
    Cost[QSM[Fix, ?]]
  }
  def TraverseQSM[T[_[_]]] = Traverse[QSM[T, ?]]
  def FunctorQSM[T[_[_]]] = Functor[QSM[T, ?]]
  def DelayRenderTreeQSM[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = implicitly[Delay[RenderTree, QSM[T, ?]]]
  def ExtractPathQSM[T[_[_]]: RecursiveT] = ExtractPath[QSM[T, ?], APath]
  def QSCoreInject[T[_[_]]] = implicitly[QScriptCore[T, ?] :<: QSM[T, ?]]
  def MonadM = Monad[M]
  def UnirewriteT[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = implicitly[Unirewrite[T, QS[T]]]
  def UnicoalesceCap[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = Unicoalesce.Capture[T, QS[T]]

  // Managed
  def MonoSeqM = MonoSeq[M]
  def ResultKvsM = Kvs[M, QueryFile.ResultHandle, XccDataStream]
  def WriteKvsM = Kvs[M, WriteFile.WriteHandle, AFile]
  def ReadKvsM = Kvs[M, ReadFile.ReadHandle, XccDataStream]

  // TODO[scalaz]: Shadow the scalaz.Monad.monadMTMAB SI-2712 workaround
  import Kleisli.kleisliMonadReader
  import WriterT.writerTMonadListen
  import BackendDef.DefErrT

  final implicit class LiftMLFSQ[A](mlfsq: MLFSQ[A]) {
    val liftQB: Backend[A] =
      mlfsq.run.liftB >>= (_.fold(
        mlerr => mlPlannerErrorToFsError(mlerr).raiseError[Backend, A],
        _.point[Backend]))
  }

  def optimize[T[_[_]]: BirecursiveT: EqualT: ShowT]
      : QSM[T, T[QSM[T, ?]]] => QSM[T, T[QSM[T, ?]]] = {
    val O = new Optimize[T]
    O.optimize(reflNT[QSM[T, ?]])
  }

  def parseConfig(uri: ConnectionUri): DefErrT[Task, Config] =
    MarkLogicConfig.fromUriString[EitherT[Task, ErrorMessages, ?]](uri.value)
      .bimap(_.left[EnvironmentError], MLBackendConfig fromMarkLogicConfig _)

  def compile(cfg: Config): DefErrT[Task, (M ~> Task, Task[Unit])] = {
    val dropWritten = λ[M ~> Free[MarkLogicFs, ?]](_.value)
    runMarkLogicFs(cfg.cfg.xccUri) map { case (f, c) => (f compose dropWritten, c) }
  }

  def rootPrefix(cfg: Config): ADir = cfg.cfg.rootDir

  def plan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](qs: T[QSM[T, ?]]): Backend[Repr] = {
    def doPlan(cfg: Config): Backend[MainModule] = {
      import cfg.{searchOptions, structuralPlannerM}
      val ejsPlanner: T[EJson] => cfg.M[XQuery] =
        EJsonPlanner.plan[T[EJson], cfg.M, cfg.FMT](_)(implicitly, structuralPlannerM, implicitly)

      MainModule.fromWritten(
        qs.cataM(cfg.planner[T].plan[Q[T]])
          .flatMap(_.fold(s =>
            Search.plan[cfg.M, Q[T], V[T], cfg.FMT](s, ejsPlanner)(
              implicitly,
              implicitly,
              implicitly,
              // NB: Not sure why these aren't found implicitly, maybe something
              //     isn't a "stable" value?
              structuralPlannerM,
              searchOptions),
            _.point[cfg.M]))
          .strengthL(Version.`1.0-ml`)
      ).liftQB
    }

    for {
      main   <- config[Backend] >>= doPlan
      pp     <- fs.ops.prettyPrint[Backend](main.queryBody)
      xqyLog =  MainModule.queryBody.modify(pp getOrElse _)(main).render
      _      <- PhaseResultTell[Backend].tell(Vector(PhaseResult.detail("XQuery", xqyLog)))
      // NB: While it would be nice to use the pretty printed body in the module
      //     returned for nicer error messages, we cannot as xdmp:pretty-print has
      //     a bug that reorders `where` and `order by` clauses in FLWOR expressions,
      //     causing them to be malformed.
    } yield main
  }

  object ManagedQueryFileModule extends ManagedQueryFileModule {
    def executePlan(repr: Repr, out: AFile): Backend[Unit] = {
      import MainModule._

      def saveResults(mm: MainModule): Backend[MainModule] = {
        val module0 =
          MonadListen_[Backend, Prologs]
            .listen(saveTo(out, mm.queryBody))

        module0 map { case (body, plogs) =>
          (prologs.modify(_ union plogs) >>> queryBody.set(body))(mm)
        }
      }

      val deleteOutIfExists =
        config[Backend] flatMap { cfg =>
          import cfg.{FMT, searchOptions}
          fs.ops.pathHavingFormatExists[M, FMT](out)
            .flatMap(_.whenM(fs.ops.deleteFile[M](out)))
            .transact
            .liftB
        }

      for {
        mm  <- saveResults(repr)
        _   <- deleteOutIfExists
        _   <- Xcc[M].execute(mm).liftB
      } yield ()
    }

    def explain(repr: Repr): Backend[String] =
      repr.render.point[Backend]

    def listContents(dir: ADir): Backend[Set[PathSegment]] =
      config[Backend] >>= { cfg =>
        import cfg.{FMT, searchOptions}
        ops.pathHavingFormatExists[M, FMT](dir).liftB.ifM(
          ops.directoryContents[M, FMT](dir).liftB,
          pathErr(pathNotFound(dir)).raiseError[Backend, Set[PathSegment]])
      }

    def fileExists(file: AFile): Configured[Boolean] =
      config[Configured] >>= { cfg =>
        import cfg.{FMT, searchOptions}
        ops.pathHavingFormatExists[M, FMT](file).liftM[ConfiguredT]
      }

    def resultsCursor(repr: Repr): Backend[XccDataStream] =
      Xcc[XccEval].evaluate(repr)
        .chunk(readChunkSize.value.toInt)
        .map(_ traverse xdmitem.decodeForFileSystem)
        .point[Backend]

    def nextChunk(c: XccDataStream): Backend[(XccDataStream, Vector[Data])] =
      MonadFsErr[Backend].unattempt(xccEvalToMLFSQ(dataStreamRead(c)).liftQB)

    def closeCursor(c: XccDataStream): Configured[Unit] =
      xccEvalToMLFSQ(dataStreamClose(c)).run.void.liftM[ConfiguredT]

    ////

    private def saveTo(dst: AFile, results: XQuery): Backend[XQuery] =
      config[Backend] >>= { cfg =>
        cfg.structuralPlannerM.seqToArray(results)
          .map(xdmp.documentInsert(pathUri(dst).xs, _))
          .liftQB
      }
  }

  object ManagedReadFileModule extends ManagedReadFileModule {
    def readCursor(file: AFile, offset: Natural, limit: Option[Positive]): Backend[XccDataStream] =
      config[Backend] >>= { cfg =>
        import cfg._
        xccEvalToMLFSQ(ops.pathHavingFormatExists[XccEval, FMT](file))
          .map(_.fold(
            ops.readFile[XccEval, FMT](file, offset, limit)
              .chunk(readChunkSize.value.toInt)
              .map(_ traverse xdmitem.decodeForFileSystem),
            Process.empty))
          .liftQB
      }

    def nextChunk(c: XccDataStream): Backend[(XccDataStream, Vector[Data])] =
      MonadFsErr[Backend].unattempt(xccEvalToMLFSQ(dataStreamRead(c)).liftQB)

    def closeCursor(c: XccDataStream): Configured[Unit] =
      xccEvalToMLFSQ(dataStreamClose(c)).run.void.liftM[ConfiguredT]
  }

  object ManagedWriteFileModule extends ManagedWriteFileModule {
    def writeCursor(file: AFile): Backend[AFile] =
      file.point[Backend]

    def writeChunk(f: AFile, chunk: Vector[Data]): Configured[Vector[FileSystemError]] =
      config[Configured] >>= { cfg =>
        import cfg._
        chunk.grouped(writeChunkSize.value.toInt)
          .toStream
          .foldMapM(write[FMT](f, _))
          .run.map(_.valueOr(mlerr => Vector(mlPlannerErrorToFsError(mlerr))))
          .liftM[ConfiguredT]
      }

    def closeCursor(f: AFile): Configured[Unit] =
      ().point[Configured]

    ////

    private def write[FMT](
      dst: AFile,
      lines: Vector[Data]
    )(implicit
      P: StructuralPlanner[MLFSQ, FMT],
      O: SearchOptions[FMT],
      C: AsContent[FMT, Data]
    ): MLFSQ[Vector[FileSystemError]] = {
      val chunk = Data._arr(lines.toList)
      ops.upsertFile[MLFSQ, FMT, Data](dst, chunk) map (_.fold(
        msgs => Vector(FileSystemError.writeFailed(chunk, msgs intercalate ", ")),
        _ map (xe => FileSystemError.writeFailed(chunk, xe.shows))))
    }
  }

  object ManageFileModule extends ManageFileModule {
    import ManageFile._

    def move(scenario: PathPair, semantics: MoveSemantics): Backend[Unit] =
      scenario match {
        case PathPair.FileToFile(src, dst) => moveFile(src, dst, semantics)
        case PathPair.DirToDir(src, dst)   => moveDir(src, dst, semantics)
      }

    def copy(pair: PathPair): Backend[Unit] =
      unsupportedOperation("Marklogic connector does not currently support copy").raiseError[Backend, Unit]

    def delete(path: APath): Backend[Unit] =
      config[Backend] >>= { cfg =>
        import cfg._
        ifExists(path)(refineType(path).fold(
          ops.deleteDir[Backend, FMT],
          ops.deleteFile[Backend])
        ).void
      }

    def tempFile(path: APath): Backend[AFile] =
      // Take my hand, scalac
      UuidReader[FileSystemErrT[PhaseResultT[Configured, ?], ?]] asks { uuid =>
        val fname = s"temp-$uuid"
        refineType(path).fold(
          d => d </> file(fname),
          f => fileParent(f) </> file(fname))
      }

    ////

    private def ifExists[A](path: APath)(thenDo: => Backend[A]): Backend[A] =
      config[Backend] >>= { cfg =>
        import cfg._
        ops.pathHavingFormatExists[Backend, FMT](path)
          .ifM(thenDo, pathErr(pathNotFound(path)).raiseError[Backend, A])
          .transact
      }

    private def checkMoveSemantics(dst: APath, sem: MoveSemantics): Backend[Unit] =
      config[Backend] >>= { cfg =>
        import cfg._
        sem match {
          case MoveSemantics.Overwrite =>
            ().point[Backend]

          case MoveSemantics.FailIfExists =>
            ops.pathHavingFormatExists[Backend, FMT](dst)
              .flatMap(_ whenM pathErr(pathExists(dst)).raiseError[Backend, Unit])

          case MoveSemantics.FailIfMissing =>
            ops.pathHavingFormatExists[Backend, FMT](dst)
              .flatMap(_ unlessM pathErr(pathNotFound(dst)).raiseError[Backend, Unit])
        }
      }

    private def moveFile(src: AFile, dst: AFile, sem: MoveSemantics): Backend[Unit] =
      config[Backend] >>= { cfg =>
        import cfg._
        ifExists(src)(
          checkMoveSemantics(dst, sem) *>
          ops.moveFile[Backend, FMT](src, dst).void)
      }

    private def moveDir(src: ADir, dst: ADir, sem: MoveSemantics): Backend[Unit] =
      config[Backend] >>= { cfg =>
        import cfg._
        ifExists(src)(
          checkMoveSemantics(dst, sem) *>
          ops.moveDir[PrologT[Backend, ?], FMT](src, dst).value.void)
      }
  }
}

object MarkLogic extends MarkLogic(10000L, 10000L)

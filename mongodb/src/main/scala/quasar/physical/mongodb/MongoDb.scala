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

package quasar
package physical.mongodb

import slamdata.Predef._
import quasar.common._
import quasar.connector._
import quasar.contrib.scalaz.MonadReader_
import quasar.contrib.pathy._
import quasar.effect.{Kvs, MonoSeq}
import quasar.fp._
import quasar.contrib.iota._
import quasar.fp.numeric._
import quasar.fp.ski.κ
import quasar.fs._
import quasar.fs.mount._
import quasar.physical.mongodb.fs.bsoncursor._
import quasar.physical.mongodb.mongoiterable._
import quasar.physical.mongodb.workflow._
import quasar.qscript._
import quasar.qscript.analysis._
import quasar.qscript.rewrites.{Optimize, Unicoalesce, Unirewrite}

import java.time.Instant
import scala.Predef.implicitly

import matryoshka._
import matryoshka.data._
import org.bson.{BsonDocument, BsonValue}
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object MongoDb
    extends BackendModule
    with ManagedReadFile[BsonCursor]
    with ManagedWriteFile[Collection]
    with DefaultAnalyzeModule {

  type QS[T[_[_]]] = fs.MongoQScriptCP[T]

  implicit def qScriptToQScriptTotal[T[_[_]]]: Injectable[QSM[T, ?], QScriptTotal[T, ?]] =
    physical.mongodb.qScriptToQScriptTotal[T]

  type Repr = Crystallized[WorkflowF]

  type M[A] = fs.MongoM[A]

  import Cost._
  import Cardinality._

  def CardinalityQSM: Cardinality[QSM[Fix, ?]] = Cardinality[QSM[Fix, ?]]
  def CostQSM: Cost[QSM[Fix, ?]] = Cost[QSM[Fix, ?]]
  def TraverseQSM[T[_[_]]] = Traverse[QSM[T, ?]]
  def FunctorQSM[T[_[_]]] = Functor[QSM[T, ?]]
  def DelayRenderTreeQSM[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = implicitly[Delay[RenderTree, QSM[T, ?]]]
  def ExtractPathQSM[T[_[_]]: RecursiveT] = ExtractPath[QSM[T, ?], APath]
  def QSCoreInject[T[_[_]]] = implicitly[QScriptCore[T, ?] :<<: QSM[T, ?]]
  def MonadM = Monad[M]
  def UnirewriteT[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = implicitly[Unirewrite[T, QS[T]]]
  def UnicoalesceCap[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] = Unicoalesce.Capture[T, QS[T]]

  type Config = fs.MongoConfig

  // Managed
  def MonoSeqM = MonoSeq[M]
  def ReadKvsM = Kvs[M, ReadFile.ReadHandle, BsonCursor]
  def WriteKvsM = Kvs[M, WriteFile.WriteHandle, Collection]

  // we don't apply `O.subsetBeforeMap` because we need to apply that
  // after we apply `mapBeforeSort`
  def optimize[T[_[_]]: BirecursiveT: EqualT: ShowT]
      : QSM[T, T[QSM[T, ?]]] => QSM[T, T[QSM[T, ?]]] = {
    val O = new Optimize[T]
    liftFFCopK[QScriptCore[T, ?], QSM[T, ?], T[QSM[T, ?]]](
      repeatedly(O.filterBeforeUnion[QSM[T, ?]]))
  }

  def parseConfig(uri: ConnectionUri): BackendDef.DefErrT[Task, Config] =
    fs.parseConfig(uri)

  def compile(cfg: Config): BackendDef.DefErrT[Task, (M ~> Task, Task[Unit])] =
    fs.compile(cfg)

  val Type = FileSystemType("mongodb")

  private def checkPathsExist[T[_[_]]: BirecursiveT](qs: T[MongoDb.QSM[T, ?]]): Backend[Unit] = {
    import fs.QueryContext._, fs.queryfileTypes.QRT
    val rez = for {
      colls <- EitherT.fromDisjunction[MongoDbIO](
                 fs.QueryContext.collections(qs).leftMap(FileSystemError.pathErr(_)))
      _     <- colls.traverse_(c => EitherT(MongoDbIO.collectionExists(c)
                .map(_ either (()) or FileSystemError.pathErr(PathError.pathNotFound(c.asFile)))))
    } yield ()
    val e: MongoLogWFR[BsonCursor, Unit] = EitherT[MongoLogWF[BsonCursor, ?], FileSystemError, Unit](
      rez.run.liftM[QRT].liftM[PhaseResultT])

    toBackendP(e.run.run)
  }

  def doPlan[
      T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT,
      N[_]: Monad: MonadFsErr: PhaseResultTell]
      (qs: T[QSM[T, ?]],
        ctx: fs.QueryContext,
        queryModel: MongoQueryModel,
        anyDoc: Collection => OptionT[N, BsonDocument],
        execTime: Instant)
      : N[Repr] =
    MongoDbPlanner.planExecTime[T, N](qs, ctx, queryModel, anyDoc, execTime)

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def plan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](
      qs: T[QSM[T, ?]]): Backend[Repr] =
    for {
      v <- config[Backend].map(_.serverVersion)
      ctx <- toBackendP(fs.QueryContext.queryContext[T, Backend](qs))
      _ <- checkPathsExist(qs)
      execTime <- QueryFileModule.queryTime.liftM[PhaseResultT].liftM[FileSystemErrT]
      anyDoc = (c: Collection) => MongoDbIO.first(c).mapT(x => toM(x).liftB)
      p <- doPlan[T, Backend](qs, ctx, MongoQueryModel(v), anyDoc, execTime)
    } yield p

  private type PhaseRes[A] = PhaseResultT[ConfiguredT[M, ?], A]

  private val effToConfigured: fs.Eff ~> Configured =
    λ[fs.Eff ~> Configured](eff => Free.liftF(eff).liftM[ConfiguredT])

  private val effToPhaseRes: fs.Eff ~> PhaseRes =
    λ[Configured ~> PhaseRes](_.liftM[PhaseResultT]) compose effToConfigured

  private def toEff[C[_], A](c: C[A])(implicit inj: C :<: fs.Eff): fs.Eff[A] = inj(c)

  def toM[C[_], A](c: C[A])(implicit inj: C :<: fs.Eff): M[A] = Free.liftF(toEff(c))

  def toBackend[C[_], A](c: C[FileSystemError \/ A])(implicit inj: C :<: fs.Eff): Backend[A] =
    EitherT(c).mapT(x => effToPhaseRes(toEff(x)))

  def toBackendP[C[_], A](c: C[(PhaseResults, FileSystemError \/ A)])(implicit inj: C :<: fs.Eff): Backend[A] =
    EitherT(WriterT(effToConfigured(toEff(c))))

  def toConfigured[C[_], A](c: C[A])(implicit inj: C :<: fs.Eff): Configured[A] =
    effToConfigured(toEff(c))

  val DC = DataCursor[MongoDbIO, BsonCursor]

  object QueryFileModule extends QueryFileModule {
    import QueryFile._

    private def mkInterp[F[_] : Functor](implicit C: MonadReader_[F, Config]): F[fs.QueryFileInterpreter] =
      config[F].map(cfg => new fs.QueryFileInterpreter(cfg.wfExec))

    def executePlan(repr: Repr, out: AFile): Backend[Unit] =
      mkInterp[Backend] >>= (i => toBackendP(i.execPlan(repr, out)))

    def evaluatePlan(repr: Repr): Backend[ResultHandle] =
      mkInterp[Backend] >>= (i => toBackendP(i.evalPlan(repr)))

    def explain(repr: Repr): Backend[String] =
      mkInterp[Backend] >>= (i => toBackendP(i.explain(repr)))

    def more(h: ResultHandle): Backend[Vector[Data]] =
      mkInterp[Backend] >>= (i => toBackend(i.more(h)))

    def close(h: ResultHandle): Configured[Unit] =
      mkInterp[Configured] >>= (i => toConfigured(i.close(h)))

    def listContents(dir: ADir): Backend[Set[PathSegment]] =
      mkInterp[Backend] >>= (i => toBackend(i.listContents0(dir)))

    def fileExists(file: AFile): Configured[Boolean] =
      mkInterp[Configured] >>= (i => toConfigured(i.fileExists(file)))

    def queryTime: Configured[Instant] =
      mkInterp[Configured] >>= (i => toConfigured(i.queryTime))

  }

  object ManagedReadFileModule extends ManagedReadFileModule {

    private def cursor(coll: Collection, offset: Natural, limit: Option[Positive]): MongoDbIO[BsonCursor] =
      for {
        iter <- MongoDbIO.find(coll)
        iter2 =  iter.skip(offset.value.toInt)
        iter3 = limit.map(l => iter2.limit(l.value.toInt)).getOrElse(iter2)
        cur  <- MongoDbIO.async(iter3.widen[BsonValue].batchCursor)
      } yield cur

    def readCursor(f: AFile, offset: Natural, limit: Option[Positive])
        : Backend[BsonCursor] =
      Collection.fromFile(f).fold(
        err  => MonadFsErr[Backend].raiseError[BsonCursor](FileSystemError.pathErr(err)),
        coll => toM(cursor(coll, offset, limit)).liftB)

    def nextChunk(c: BsonCursor): Backend[(BsonCursor, Vector[Data])] =
      toM(DC.nextChunk(c).map((c, _))).liftB

    def closeCursor(c: BsonCursor): Configured[Unit] =
      toConfigured(DC.close(c))
  }

  object ManagedWriteFileModule extends ManagedWriteFileModule {
    private def dataToDocument(v: BsonVersion, d: Data): FileSystemError \/ Bson.Doc =
      BsonCodec.fromData(v, d)
        .leftMap(err => FileSystemError.writeFailed(d, err.shows))
        .flatMap {
          case doc @ Bson.Doc(_) => doc.right
          case otherwise         => FileSystemError.writeFailed(d, "MongoDB is only able to store documents").left
        }

    def writeCursor(file: AFile): Backend[Collection] =
      Collection.fromFile(file).fold(
        err => MonadFsErr[Backend].raiseError[Collection](FileSystemError.pathErr(err)),
        coll => toM(MongoDbIO.ensureCollection(coll) *> coll.point[MongoDbIO]).liftB)

    def writeChunk(c: Collection, chunk: Vector[Data])
        : Configured[Vector[FileSystemError]] =
      for {
        v <- config[Configured].map(cfg => MongoQueryModel.toBsonVersion(MongoQueryModel(cfg.serverVersion)))
        r <- doWriteChunk(v, c, chunk)
      } yield r

    private def doWriteChunk(v: BsonVersion, c: Collection, chunk: Vector[Data])
        : Configured[Vector[FileSystemError]] = {
      val (errs, docs) = chunk foldMap { d =>
        dataToDocument(v, d).fold(
          e => (Vector(e), Vector()),
          d => (Vector(), Vector(d)))
      }
      val io = MongoDbIO.insertAny(c, docs.map(_.repr))
        .filter(_ < docs.size)
        .map(n => FileSystemError.partialWrite(docs.size - n))
        .run.map(errs ++ _.toList)
      toConfigured(io)
    }

    def closeCursor(c: Collection): Configured[Unit] =
      ().point[Configured]
  }

  object ManageFileModule extends ManageFileModule {
    import fs.managefile._, ManageFile._

    /** TODO: There are still some questions regarding Path
      *   1) We should assume all paths will be canonicalized and can do so
      *      with a ManageFile ~> ManageFile that canonicalizes everything.
      *
      *   2) Currently, parsing a directory like "/../foo/bar/" as an absolute
      *      dir succeeds, this should probably be changed to fail.
      */
    def move(scenario: PathPair, semantics: MoveSemantics): Backend[Unit] = {
      val mm: MongoManage[FileSystemError \/ Unit] =
        scenario.fold(moveDir(_, _, semantics), moveFile(_, _, semantics))
          .run.liftM[ManageInT]
      toBackend(mm)
    }

    def copy(pair: PathPair): Backend[Unit] =
      toBackend(FileSystemError.unsupportedOperation("MongoDb connector does not currently support copying").left[Unit].point[MongoDbIO])

    def delete(path: APath): Backend[Unit] = {
      val mm: MongoManage[FileSystemError \/ Unit] =
        pathy.Path.refineType(path).fold(deleteDir, deleteFile)
          .run.liftM[ManageInT]
      toBackend(mm)
    }

    def tempFile(near: APath, prefix: Option[TempFilePrefix]): Backend[AFile] = {
      val checkPath =
        EitherT.fromDisjunction[MongoManage](Collection.dbNameFromPath(near))
          .bimap(FileSystemError.pathErr(_), κ(()))

      val mm: MongoManage[FileSystemError \/ AFile] =
        checkPath.flatMap(κ(freshFile(near, prefix).liftM[FileSystemErrT])).run

      toBackend(mm)
    }
  }
}

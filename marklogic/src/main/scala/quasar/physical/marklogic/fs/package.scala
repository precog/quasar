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

package quasar.physical.marklogic

import quasar.Predef.{uuid => _, _}
import quasar.{Data, Planner => QPlanner}
import quasar.common._
import quasar.connector.EnvironmentError
import quasar.contrib.pathy._
import quasar.contrib.scalaz.catchable._
import quasar.contrib.scalaz.eitherT._
import quasar.contrib.scalaz.writerT._
import quasar.effect._
import quasar.fp._, free._
import quasar.fp.numeric.Positive
import quasar.frontend.logicalplan
import quasar.fs._
import quasar.fs.impl.DataStream
import quasar.fs.mount._, FileSystemDef.{DefinitionError, DefinitionResult, DefErrT}
import quasar.physical.marklogic.qscript._
import quasar.physical.marklogic.xcc.{AsContent, provideSession}
import quasar.physical.marklogic.xquery.PrologT

import java.net.URI
import java.util.UUID

import com.marklogic.xcc._
import com.marklogic.xcc.exceptions._
import matryoshka.data.Fix
import pathy.Path.{rootDir => pRootDir, _}
import scalaz.{Failure => _, _}, Scalaz.{ToIdOps => _, _}
import scalaz.concurrent.Task

package object fs {
  import ReadFile.ReadHandle, WriteFile.WriteHandle, QueryFile.ResultHandle
  import queryfile.MLQScript
  import xcc._
  import uuid.GenUUID

  type XccSessionR[A]       = Read[Session, A]
  type XccContentSourceR[A] = Read[ContentSource, A]

  type XccEvalEff[A] = (
        Task
    :\: MonotonicSeq
    :\: XccSessionR
    :/: XccContentSourceR
  )#M[A]

  type XccEval[A]         = MarkLogicPlanErrT[PrologT[Free[XccEvalEff, ?], ?], A]
  type XccDataStream      = DataStream[XccEval]

  type MLReadHandles[A]   = KeyValueStore[ReadHandle, XccDataStream, A]
  type MLResultHandles[A] = KeyValueStore[ResultHandle, XccDataStream, A]
  type MLWriteHandles[A]  = KeyValueStore[WriteHandle, Unit, A]

  type MarkLogicFs[A] = (
        GenUUID
    :\: MLReadHandles
    :\: MLResultHandles
    :\: MLWriteHandles
    :/: XccEvalEff
  )#M[A]

  type MLFS[A]  = PrologT[Free[MarkLogicFs, ?], A]
  type MLFSQ[A] = MarkLogicPlanErrT[MLFS, A]

  private implicit val xccSessionR  = Read.monadReader_[Session, XccEvalEff]
  private implicit val xccSourceR   = Read.monadReader_[ContentSource, XccEvalEff]

  private implicit val mlfsSessionR = Read.monadReader_[Session, MarkLogicFs]
  private implicit val mlfsCSourceR = Read.monadReader_[ContentSource, MarkLogicFs]
  private implicit val mlfsUuidR    = Read.monadReader_[UUID, MarkLogicFs]

  val FsType = FileSystemType("marklogic")

  /** The `ContentSource` located at the given URI. */
  def contentSourceAt[F[_]: Capture](uri: URI): F[ContentSource] =
    Capture[F].capture(ContentSourceFactory.newContentSource(uri))

  /** The `ContentSource` located at the given ConnectionUri. */
  def contentSourceConnection[F[_]: Capture: Bind](connectionUri: ConnectionUri): F[ContentSource] =
    Capture[F].capture(new URI(connectionUri.value)) >>= contentSourceAt[F]

  // TODO: Make these URI query parameters?
  def definition(readChunkSize: Positive, writeChunkSize: Positive): FileSystemDef[Task] =
    FileSystemDef fromPF {
      case (FsType, uri) =>
        MarkLogicConfig.fromUriString[EitherT[Task, ErrorMessages, ?]](uri.value)
          .leftMap(_.left[EnvironmentError])
          .flatMap(cfg => cfg.docType.fold(
            fileSystem[DocType.Json](cfg.xccUri, cfg.rootDir, readChunkSize, writeChunkSize),
            fileSystem[DocType.Xml](cfg.xccUri, cfg.rootDir, readChunkSize, writeChunkSize)))
    }

  /** The MarkLogic FileSystem definition.
    *
    * @tparam FMT            type representing the document format for the filesystem
    * @param  xccUri         the URI describing the details of the connection to the XCC server
    * @param  rootDir        the MarkLogic directory upon which to base the mount
    * @param  readChunkSize  the size of a single chunk when streaming records from MarkLogic
    * @param  writeChunkSize the size of a single chunk when streaming records to MarkLogic
    */
  def fileSystem[FMT: SearchOptions](
    xccUri: URI,
    rootDir: ADir,
    readChunkSize: Positive,
    writeChunkSize: Positive
  )(implicit
    C : AsContent[FMT, Data],
    P : Planner[MLFSQ, FMT, MLQScript[Fix, ?]],
    SP: StructuralPlanner[XccEval, FMT]
  ): DefErrT[Task, DefinitionResult[Task]] = {
    implicit val mlfsqStructuralPlanner: StructuralPlanner[MLFSQ, FMT] =
      SP.transform(xccEvalToMLFSQ)

    val dropWritten = λ[MLFS ~> Free[MarkLogicFs, ?]](_.value)

    val xformPaths =
      if (rootDir === pRootDir) liftFT[FileSystem]
      else chroot.fileSystem[FileSystem](rootDir)

    runMarkLogicFs(xccUri) map { case (run, shutdown) =>
      DefinitionResult[Task](
        run compose dropWritten compose foldMapNT(interpretFileSystem(
          convertQueryFileErrors(queryfile.interpret[XccEval, MLFSQ, FMT](
            readChunkSize, xccEvalToMLFSQ)),
          convertReadFileErrors(readfile.interpret[XccEval, MLFSQ, FMT](
            readChunkSize, xccEvalToMLFSQ)),
          convertWriteFileErrors(writefile.interpret[MLFSQ, FMT](writeChunkSize)),
          managefile.interpret[MLFS, FMT]
        )) compose xformPaths, shutdown)
    }
  }

  /** Converts MarkLogicPlannerErrors into FileSystemErrors. */
  def mlPlannerErrorToFsError(mlerr: MarkLogicPlannerError): FileSystemError = {
    import MarkLogicPlannerError._

    def unsupportedPlan(desc: String): FileSystemError =
      FileSystemError.qscriptPlanningFailed(QPlanner.UnsupportedPlan(
        logicalplan.constant(Data.Str(desc)), Some(mlerr.shows)))

    mlerr match {
      case InvalidQName(s)  => unsupportedPlan(s)
      case Unimplemented(f) => unsupportedPlan(f)

      case Unreachable(d)   =>
        FileSystemError.qscriptPlanningFailed(QPlanner.InternalError(
          s"Should not have been reached, indicates a defect: $d.",
          None))
    }
  }

  /** The URI representation of the given path. */
  def pathUri(path: APath): String =
    UriPathCodec.printPath(path)

  // TODO: Refactor effect interpreters in terms of abstractions like `Catchable` and `Capture`, etc
  //       so this doesn't have to depend on `Task`.
  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def runMarkLogicFs(xccUri: URI): DefErrT[Task, (Free[MarkLogicFs, ?] ~> Task, Task[Unit])] = {
    type CRT[X[_], A] = ReaderT[X, ContentSource, A]
    type  CR[      A] = CRT[Task, A]
    type SRT[X[_], A] = ReaderT[X, Session, A]
    type  SR[      A] = SRT[Task, A]

    val contentSource: DefErrT[Task, ContentSource] =
      EitherT(contentSourceAt[Task](xccUri) map (_.right[DefinitionError]) handleNonFatal {
        case th => th.getMessage.wrapNel.left[EnvironmentError].left
      })

    val runFs = (
      KeyValueStore.impl.empty[WriteHandle, Unit]           |@|
      KeyValueStore.impl.empty[ReadHandle, XccDataStream]   |@|
      KeyValueStore.impl.empty[ResultHandle, XccDataStream] |@|
      MonotonicSeq.fromZero                                 |@|
      GenUUID.type4[Task]
    ).tupled.map { case (whandles, rhandles, qhandles, seq, genUUID) =>
      contentSource flatMapF { cs =>
        val mlToSR = (liftMT[Task, SRT] compose genUUID)  :+:
                     (liftMT[Task, SRT] compose rhandles) :+:
                     (liftMT[Task, SRT] compose qhandles) :+:
                     (liftMT[Task, SRT] compose whandles) :+:
                      liftMT[Task, SRT]                   :+:
                     (liftMT[Task, SRT] compose seq)      :+:
                     Read.toReader[SR, Session]           :+:
                     Read.constant[SR, ContentSource](cs)
        val runML  = provideSession[Task](cs) compose foldMapNT(mlToSR)
        val sdown  = Task.delay(cs.getConnectionProvider.shutdown(null))

        contentsource.defaultSession[CR] flatMap {
          testXccConnection[SRT[CR, ?]]
            .map(_.right[NonEmptyList[String]])
            .toLeft((runML, sdown))
            .run
        } handleNonFatal {
          case th => EnvironmentError.connectionFailed(th).right.left
        } run cs
      }
    }

    runFs.liftM[DefErrT].join
  }

  /** Returns an error describing why an XCC connection failed or `None` if it
    * is successful.
    */
  def testXccConnection[F[_]: Applicative](implicit X: Xcc[F]): OptionT[F, EnvironmentError] =
    // NB: An innocuous operation used to test the connection.
    OptionT(X.handle(X.currentServerPointInTime as none[EnvironmentError]) {
      case XccError.RequestError(ex: RequestPermissionException) =>
        EnvironmentError.invalidCredentials(ex.getMessage).some

      case other =>
        EnvironmentError.connectionFailed(XccError.cause.get(other)).some
    })

  /** Lift XccEval into MLFSQ. */
  val xccEvalToMLFSQ: XccEval ~> MLFSQ =
    Hoist[MarkLogicPlanErrT].hoist(Hoist[PrologT].hoist(mapSNT(Inject[XccEvalEff, MarkLogicFs])))

  implicit val dataAsXmlContent: AsContent[DocType.Xml, Data] =
    new AsContent[DocType.Xml, Data] {
      def asContent[F[_]: MonadErrMsgs](uri: ContentUri, d: Data): F[Content] =
        data.encodeXml[F](d) map { elem =>
          val opts = new ContentCreateOptions
          opts.setFormatXml()
          ContentFactory.newContent(uri.value, elem.toString, opts)
        }
    }

  implicit val dataAsJsonContent: AsContent[DocType.Json, Data] =
    new AsContent[DocType.Json, Data] {
      def asContent[F[_]: MonadErrMsgs](uri: ContentUri, d: Data): F[Content] = {
        val opts = new ContentCreateOptions
        opts.setFormatJson()
        ContentFactory.newContent(uri.value, data.encodeJson(d).nospaces, opts).point[F]
      }
    }

  ////

  private def asFsError[F[_]: Functor, G[_]: Applicative, A](
    fa: MarkLogicPlanErrT[F, G[FileSystemError \/ A]]
  ): F[G[FileSystemError \/ A]] =
    fa.valueOr(mlerr => mlPlannerErrorToFsError(mlerr).left[A].point[G])

  private def convertQueryFileErrors[F[_]: Functor](f: QueryFile ~> MarkLogicPlanErrT[F, ?]): QueryFile ~> F = {
    import QueryFile._
    def handleMLErr[A] = asFsError[F, (PhaseResults, ?), A](_)

    λ[QueryFile ~> F] {
      case ExecutePlan(lp, file) => handleMLErr(f(ExecutePlan(lp, file)))
      case EvaluatePlan(lp)      => handleMLErr(f(EvaluatePlan(lp)))
      case More(h)               => f(More(h)) | Vector[Data]().right[FileSystemError]
      case Close(h)              => f(Close(h)) | (())
      case Explain(lp)           => handleMLErr(f(Explain(lp)))
      case ListContents(dir)     => f(ListContents(dir)) | Set[PathSegment]().right[FileSystemError]
      case FileExists(file)      => f(FileExists(file)) | false
    }
  }

  private def convertReadFileErrors[F[_]: Functor](f: ReadFile ~> MarkLogicPlanErrT[F, ?]): ReadFile ~> F = {
    import ReadFile._
    def handleMLErr[A] = asFsError[F, Id, A](_)

    λ[ReadFile ~> F] {
      case Open(file, off, lim) => handleMLErr(f(Open(file, off, lim)))
      case Read(h)              => handleMLErr(f(Read(h)))
      case Close(h)             => f(Close(h)) | (())
    }
  }

  private def convertWriteFileErrors[F[_]: Functor](f: WriteFile ~> MarkLogicPlanErrT[F, ?]): WriteFile ~> F = {
    import WriteFile._

    λ[WriteFile ~> F] {
      case Open(file)  => asFsError[F, Id, WriteHandle](f(Open(file)))
      case Write(h, d) => f(Write(h, d)) valueOr (e => Vector(mlPlannerErrorToFsError(e)))
      case Close(h)    => f(Close(h)) | (())
    }
  }
}

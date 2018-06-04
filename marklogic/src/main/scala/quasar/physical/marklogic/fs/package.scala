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

package quasar.physical.marklogic

import slamdata.Predef.{uuid => _, _}
import quasar.Data
import quasar.contrib.pathy._
import quasar.contrib.scalaz.catchable._
import quasar.contrib.scalaz.eitherT._
import quasar.effect._
import quasar.fp._, free._
import quasar.frontend.logicalplan
import quasar.fs.{Planner => QPlanner, _}
import quasar.fs.impl.DataStream
import quasar.fs.mount._, BackendDef.{DefinitionError, DefErrT}
import quasar.physical.marklogic.qscript._
import quasar.physical.marklogic.xcc.{AsContent, provideSession}
import quasar.physical.marklogic.xquery.PrologT
import quasar.{qscript => qs}

import java.net.URI

import com.marklogic.xcc._
import com.marklogic.xcc.exceptions._
import com.marklogic.xcc.{ContentSource, Session}
import scalaz.{Failure => _, _}, Scalaz.{ToIdOps => _, _}
import scalaz.concurrent.Task
import iotaz.TListK.:::
import iotaz.TNilK

package object fs {
  import ReadFile.ReadHandle, WriteFile.WriteHandle, QueryFile.ResultHandle
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
  type MLWriteHandles[A]  = KeyValueStore[WriteHandle, AFile, A]

  type MarkLogicFs[A] = (
        GenUUID
    :\: MLReadHandles
    :\: MLResultHandles
    :\: MLWriteHandles
    :/: XccEvalEff
  )#M[A]

  implicit val xccSessionR  = quasar.effect.Read.monadReader_[Session, XccEvalEff]
  implicit val xccSourceR   = quasar.effect.Read.monadReader_[ContentSource, XccEvalEff]
  implicit val mlfsSessionR = quasar.effect.Read.monadReader_[Session, MarkLogicFs]
  implicit val mlfsCSourceR = quasar.effect.Read.monadReader_[ContentSource, MarkLogicFs]
  implicit val mlfsUuidR    = quasar.effect.Read.monadReader_[UUID, MarkLogicFs]

  type MLFS[A]  = PrologT[Free[MarkLogicFs, ?], A]
  type MLFSQ[A] = MarkLogicPlanErrT[MLFS, A]

  type MLQScriptCP[T[_[_]]] =
    qs.QScriptCore[T, ?]           :::
    qs.ThetaJoin[T, ?]             :::
    Const[qs.ShiftedRead[ADir], ?] :::
    Const[qs.Read[AFile], ?]       :::
    TNilK

  val FsType = FileSystemType("marklogic")

  /** The `ContentSource` located at the given URI. */
  def contentSourceAt[F[_]: Capture](uri: URI): F[ContentSource] =
    Capture[F].capture(ContentSourceFactory.newContentSource(uri))

  /** The `ContentSource` located at the given ConnectionUri. */
  def contentSourceConnection[F[_]: Capture: Bind](connectionUri: ConnectionUri): F[ContentSource] =
    Capture[F].capture(new URI(connectionUri.value)) >>= contentSourceAt[F]

  /** Converts MarkLogicPlannerErrors into FileSystemErrors. */
  def mlPlannerErrorToFsError(mlerr: MarkLogicPlannerError): FileSystemError = {
    import MarkLogicPlannerError._

    def unsupportedPlan(desc: String): FileSystemError =
      FileSystemError.qscriptPlanningFailed(QPlanner.UnsupportedPlan(
        logicalplan.constant(Data.Str(desc)), Some(mlerr.shows)))

    mlerr match {
      case InvalidQName(s)  => unsupportedPlan(s)
      case InvalidUri(s)    => unsupportedPlan(s)
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
      KeyValueStore.impl.default[WriteHandle, AFile]          |@|
      KeyValueStore.impl.default[ReadHandle, XccDataStream]   |@|
      KeyValueStore.impl.default[ResultHandle, XccDataStream] |@|
      MonotonicSeq.from(0L)                                   |@|
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
}

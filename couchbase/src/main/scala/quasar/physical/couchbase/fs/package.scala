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

package quasar.physical.couchbase

import slamdata.Predef._
import quasar.connector.EnvironmentError, EnvironmentError.invalidCredentials
import quasar.effect.{KeyValueStore, MonotonicSeq, Read}
import quasar.effect.uuid.GenUUID
import quasar.fp._, free._, ski.κ
import quasar.fs._, ReadFile.ReadHandle, WriteFile.WriteHandle, QueryFile.ResultHandle
import quasar.fs.mount._, FileSystemDef.{DefErrT, DefinitionError}

import quasar.physical.couchbase.common.{ClientContext, Cursor, DocTypeKey}

import java.time.Duration
import scala.math

import com.couchbase.client.core.CouchbaseException
import com.couchbase.client.java.{CouchbaseCluster, Cluster}
import com.couchbase.client.java.env.{CouchbaseEnvironment, DefaultCouchbaseEnvironment}
import com.couchbase.client.java.util.features.Version
import org.http4s.{Uri, Query}
import scalaz._, Scalaz._
import scalaz.concurrent.Task

// TODO: Injection? Parameterized queries could help but don't appear to handle bucket names within ``

package object fs {
  val FsType = FileSystemType("couchbase")

  val minimumRequiredVersion = new Version(4, 5, 1)

  val defaultSocketConnectTimeout: Duration = Duration.ofSeconds(1)
  val defaultQueryTimeout: Duration         = Duration.ofSeconds(300)

  type Eff[A] = (
    Task                                             :\:
    Read[ClientContext, ?]                           :\:
    MonotonicSeq                                     :\:
    GenUUID                                          :\:
    KeyValueStore[ReadHandle,   Cursor,  ?]          :\:
    KeyValueStore[WriteHandle,  writefile.State,  ?] :/:
    KeyValueStore[ResultHandle, Cursor, ?]
  )#M[A]

  def context(connectionUri: ConnectionUri): DefErrT[Task, (ClientContext, Cluster)] = {
    final case class ConnUriParams(bucket: String, pass: String, docTypeKey: String, socketConnectTimeout: Duration, queryTimeout: Duration)

    def liftDT[A](v: => NonEmptyList[String] \/ A): DefErrT[Task, A] =
      EitherT(Task.delay(
        v.leftMap(_.left[EnvironmentError])
      ))

    def env(p: ConnUriParams): Task[CouchbaseEnvironment] = Task.delay(
      DefaultCouchbaseEnvironment
        .builder()
        .socketConnectTimeout(
          math.min(p.socketConnectTimeout.toMillis, Int.MaxValue.toLong).toInt)
        .queryTimeout(p.queryTimeout.toMillis)
        .build())

    def duration(uri: Uri, name: String, default: Duration) =
      (uri.params.get(name) ∘ (parseLong(_) ∘ Duration.ofSeconds))
         .getOrElse(default.success)
         .leftMap(κ(s"$name must be a valid long".wrapNel))

    for {
      uri     <- liftDT(
                   Uri.fromString(connectionUri.value).leftMap(_.message.wrapNel)
                 )
      params  <- liftDT((
                   uri.params.get("password").toSuccessNel("No password in ConnectionUri")   |@|
                   uri.params.get("docTypeKey").toSuccessNel("No doctype in ConnectionUri")  |@|
                   duration(uri, "socketConnectTimeoutSeconds", defaultSocketConnectTimeout) |@|
                   duration(uri, "queryTimeoutSeconds", defaultQueryTimeout)
                 )(ConnUriParams(uri.path.stripPrefix("/"), _, _, _, _)).disjunction)
      ev      <- env(params).liftM[DefErrT]
      cluster <- EitherT(Task.delay(
                   CouchbaseCluster.fromConnectionString(ev, uri.copy(path = "", query = Query.empty).renderString).right
                 ).handle {
                   case e: Exception => e.getMessage.wrapNel.left[EnvironmentError].left
                 })
      cm      =  cluster.clusterManager(params.bucket, params.pass)
      _       <- EitherT(Task.delay(
                   (cm.info.getMinVersion.compareTo(minimumRequiredVersion) >= 0).unlessM(
                     s"Couchbase Server must be ${minimumRequiredVersion}+"
                       .wrapNel.left[EnvironmentError].left)
                 ).handle {
                   // NB: CB client issues an opque error when credentials or bucket are incorrect 
                   case _: CouchbaseException => 
                     invalidCredentials(
                       "Unable to obtain a ClusterManager with provided bucket name or password."
                     ).right[NonEmptyList[String]].left
                 })
      bkt     <- EitherT(Task.delay(cm.hasBucket(params.bucket).booleanValue).ifM(
                   Task.delay(cluster.openBucket(params.bucket, params.pass).right[DefinitionError]),
                   Task.now(s"Bucket ${params.bucket} not found".wrapNel.left.left)))
    } yield (ClientContext(bkt, DocTypeKey(params.docTypeKey)), cluster)
  }

  def interp[S[_]](
    connectionUri: ConnectionUri
  )(implicit
    S0: Task :<: S
  ): DefErrT[Free[S, ?], (Free[Eff, ?] ~> Free[S, ?], Free[S, Unit])] = {

    def taskInterp(
      ctx: ClientContext,
      cluster: Cluster
    ): Task[(Free[Eff, ?] ~> Free[S, ?], Free[S, Unit])]  =
      (TaskRef(Map.empty[ReadHandle,   Cursor])          |@|
       TaskRef(Map.empty[WriteHandle,  writefile.State]) |@|
       TaskRef(Map.empty[ResultHandle, Cursor])          |@|
       TaskRef(0L)                                       |@|
       GenUUID.type1[Task]
     )((kvR, kvW, kvQ, i, genUUID) =>
      (
        mapSNT(injectNT[Task, S] compose (
          reflNT[Task]                            :+:
          Read.constant[Task, ClientContext](ctx) :+:
          MonotonicSeq.fromTaskRef(i)             :+:
          genUUID                                 :+:
          KeyValueStore.impl.fromTaskRef(kvR)     :+:
          KeyValueStore.impl.fromTaskRef(kvW)     :+:
          KeyValueStore.impl.fromTaskRef(kvQ))),
        lift(Task.delay(cluster.disconnect()).void).into
      ))

    EitherT(lift(context(connectionUri).run >>= (_.traverse((taskInterp _).tupled))).into)
  }

  def definition[S[_]](implicit
      S0: Task :<: S,
      S1: PhysErr :<: S
    ): FileSystemDef[Free[S, ?]] =
    FileSystemDef.fromPF {
      case (FsType, uri) =>
        interp(uri).map { case (run, close) =>
          FileSystemDef.DefinitionResult[Free[S, ?]](
            run compose interpretAnalyticalFileSystem(
              // FIX-ME
              Empty.analyze[Free[Eff, ?]],
              queryfile.interpret,
              readfile.interpret,
              writefile.interpret,
              managefile.interpret),
            close)
        }
    }
}

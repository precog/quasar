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
import quasar.connector.EnvironmentError, EnvironmentError.{invalidCredentials}
import quasar.effect.{KeyValueStore, MonotonicSeq}
import quasar.effect.uuid.GenUUID
import quasar.fp._, free._, ski.κ
import quasar.fs._, ReadFile.ReadHandle, WriteFile.WriteHandle, QueryFile.ResultHandle
import quasar.fs.mount._, BackendDef.{DefErrT, DefinitionError}

import java.time.Duration
import scala.collection.JavaConverters._
import scala.math

import com.couchbase.client.core.CouchbaseException
import com.couchbase.client.java.CouchbaseCluster
import com.couchbase.client.java.env.{CouchbaseEnvironment, DefaultCouchbaseEnvironment}
import com.couchbase.client.java.util.features.Version
import org.http4s.{Uri, Query}
import scalaz._, Scalaz._
import scalaz.concurrent.Task

package object fs {
  import common._
  import Couchbase.{Config, Eff, M}

  val FsType = FileSystemType("couchbase")

  val minimumRequiredVersion = new Version(4, 5, 1)

  val defaultSocketConnectTimeout: Duration = Duration.ofSeconds(1)
  val defaultQueryTimeout: Duration         = Duration.ofSeconds(300)

  def parseConfig(connectionUri: ConnectionUri): DefErrT[Task, Config] = {
    final case class ConnUriParams(
      bucket: String, pass: String, docTypeKey: String,
      socketConnectTimeout: Duration, queryTimeout: Duration)

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

    implicit class LiftDT[A](v: => NonEmptyList[String] \/ A) {
      def liftDT: DefErrT[Task, A] = EitherT(Task.delay(v.leftMap(_.left[EnvironmentError])))
    }

    for {
      uri     <- Uri.fromString(connectionUri.value).leftMap(_.message.wrapNel).liftDT
      params  <- (
                   uri.params.get("password").toSuccessNel("No password in ConnectionUri")     |@|
                   uri.params.get("docTypeKey").toSuccessNel("No docTypeKey in ConnectionUri") |@|
                   duration(uri, "socketConnectTimeoutSeconds", defaultSocketConnectTimeout)   |@|
                   duration(uri, "queryTimeoutSeconds", defaultQueryTimeout)
                 )(ConnUriParams(uri.path.stripPrefix("/"), _, _, _, _)).disjunction.liftDT
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
                   // NB: CB client issues an opaque error when credentials or bucket are incorrect
                   case _: CouchbaseException =>
                     invalidCredentials(
                       "Unable to obtain a ClusterManager with provided bucket name or password."
                     ).right[NonEmptyList[String]].left
                 })
      bkt     <- EitherT(Task.delay(cm.hasBucket(params.bucket).booleanValue).ifM(
                   Task.delay(cluster.openBucket(params.bucket, params.pass).right[DefinitionError]),
                   Task.now(s"Bucket ${params.bucket} not found".wrapNel.left.left)))
      bktMgr  =  bkt.bucketManager
      idxExst <- Task.delay(
                   bktMgr.listN1qlIndexes.asScala.find { i =>
                     \/.fromTryCatchThrowable[Boolean, Throwable](
                       i.indexKey.getString(0) ≟ s"`${params.docTypeKey}`"
                     ).valueOr(κ(false))
                   }.isDefined).liftM[DefErrT]
      _       <- idxExst.unlessM(Task.delay(bktMgr.createN1qlIndex(
                   s"quasar_${params.docTypeKey}_idx", true, false, params.docTypeKey))).liftM[DefErrT]
      lcv     =  ListContentsView(DocTypeKey(params.docTypeKey))
      _       <- Task.delay(bktMgr.upsertDesignDocument(lcv.designDoc)).liftM[DefErrT]
    } yield Config(ClientContext(bkt, DocTypeKey(params.docTypeKey), lcv), cluster)
  }

  def interp: Task[Eff ~> Task] =
    (
      TaskRef(Map.empty[ReadHandle, Cursor])      |@|
      TaskRef(Map.empty[WriteHandle, Collection]) |@|
      TaskRef(Map.empty[ResultHandle, Cursor])    |@|
      TaskRef(0L)                                 |@|
      GenUUID.type1[Task]
    )((kvR, kvW, kvQ, i, genUUID) =>
      reflNT[Task]                        :+:
      MonotonicSeq.fromTaskRef(i)         :+:
      genUUID                             :+:
      KeyValueStore.impl.fromTaskRef(kvR) :+:
      KeyValueStore.impl.fromTaskRef(kvW) :+:
      KeyValueStore.impl.fromTaskRef(kvQ))

  def compile(cfg: Config): DefErrT[Task, (M ~> Task, Task[Unit])] =
    (interp ∘ (i => (foldMapNT[Eff, Task](i), Task.delay(cfg.cluster.disconnect()).void)))
      .liftM[DefErrT]
}

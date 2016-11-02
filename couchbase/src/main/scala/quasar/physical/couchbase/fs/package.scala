/*
 * Copyright 2014–2016 SlamData Inc.
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

import quasar.Predef._
import quasar.connector.EnvironmentError, EnvironmentError.{connectionFailed, invalidCredentials}
import quasar.effect.{KeyValueStore, MonotonicSeq, Read}
import quasar.effect.uuid.GenUUID
import quasar.fp._, free._
import quasar.fs._, ReadFile.ReadHandle, WriteFile.WriteHandle, QueryFile.ResultHandle
import quasar.fs.mount._, FileSystemDef.DefErrT
import quasar.physical.couchbase.common.{Context, Cursor}

import java.net.ConnectException
import java.util.concurrent.TimeUnit.SECONDS

import com.couchbase.client.java.CouchbaseCluster
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment
import com.couchbase.client.java.error.InvalidPasswordException
import org.http4s.Uri
import scalaz._, Scalaz._
import scalaz.concurrent.Task

// TODO: Injection? Parameterized queries could help but don't appear to handle bucket names within ``
// TODO: Handle query returned errors field

package object fs {
  val FsType = FileSystemType("couchbase")

  type Eff[A] = (
    Task                                             :\:
    Read[Context, ?]                                 :\:
    MonotonicSeq                                     :\:
    GenUUID                                          :\:
    KeyValueStore[ReadHandle,   Cursor,  ?]          :\:
    KeyValueStore[WriteHandle,  writefile.State,  ?] :/:
    KeyValueStore[ResultHandle, Cursor, ?]
  )#M[A]

  object CBConnectException {
    def unapply(ex: Throwable): Option[ConnectException] =
      ex.getCause match {
        case ex: ConnectException => ex.some
        case _                    => none
      }
  }

  def interp[S[_]](
      connectionUri: ConnectionUri
    )(implicit
      S0: Task :<: S
    ): DefErrT[Free[S, ?], (Free[Eff, ?] ~> Free[S, ?], Free[S, Unit])] = {

    final case class ConnUriParams(user: String, pass: String)

    def liftDT[A](v: NonEmptyList[String] \/ A): DefErrT[Task, A] =
      EitherT.fromDisjunction[Task](v.leftMap(_.left[EnvironmentError]))

    // TODO: retrieve from connectionUri params
    val env = DefaultCouchbaseEnvironment
      .builder()
      .queryTimeout(SECONDS.toMillis(150))
      .build()

    val cbCtx: DefErrT[Task, Context] =
      for {
        uri     <- liftDT(
                     Uri.fromString(connectionUri.value).leftMap(_.message.wrapNel)
                   )
        cluster <- EitherT(Task.delay(
                     CouchbaseCluster.fromConnectionString(env, uri.renderString).right
                   ).handle {
                     case e: Exception => e.getMessage.wrapNel.left[EnvironmentError].left
                   })
        params  <- liftDT((
                     uri.params.get("username").toSuccessNel("No username in ConnectionUri") |@|
                     uri.params.get("password").toSuccessNel("No password in ConnectionUri")
                   )(ConnUriParams).disjunction)
        cm      =  cluster.clusterManager(params.user, params.pass)
        _       <- EitherT(Task.delay(
                     // verify credentials via call to info
                     cm.info
                   ).as(
                     ().right
                   ).handle {
                     case _: InvalidPasswordException =>
                       invalidCredentials(
                         "Unable to obtain a ClusterManager with provided credentials."
                       ).right[NonEmptyList[String]].left
                     case CBConnectException(ex) =>
                       connectionFailed(
                         ex
                       ).right[NonEmptyList[String]].left
                   })
      } yield Context(cluster, cm)

    def taskInterp(
      ctx: Context
    ): Task[(Free[Eff, ?] ~> Free[S, ?], Free[S, Unit])]  =
      (TaskRef(Map.empty[ReadHandle,   Cursor])          |@|
       TaskRef(Map.empty[WriteHandle,  writefile.State]) |@|
       TaskRef(Map.empty[ResultHandle, Cursor])          |@|
       TaskRef(0L)                                       |@|
       GenUUID.type1
     )((kvR, kvW, kvQ, i, genUUID) =>
      (
        mapSNT(injectNT[Task, S] compose (
          reflNT[Task]                          :+:
          Read.constant[Task, Context](ctx)     :+:
          MonotonicSeq.fromTaskRef(i)           :+:
          genUUID                               :+:
          KeyValueStore.impl.fromTaskRef(kvR)   :+:
          KeyValueStore.impl.fromTaskRef(kvW)   :+:
          KeyValueStore.impl.fromTaskRef(kvQ))),
        lift(Task.delay(ctx.cluster.disconnect()).void).into
      ))

    EitherT(lift(cbCtx.run >>= (_.traverse(taskInterp))).into)
  }

  def definition[S[_]](implicit
      S0: Task :<: S,
      S1: PhysErr :<: S
    ): FileSystemDef[Free[S, ?]] =
    FileSystemDef.fromPF {
      case (FsType, uri) =>
        interp(uri).map { case (run, close) =>
          FileSystemDef.DefinitionResult[Free[S, ?]](
            run compose interpretFileSystem(
              queryfile.interpret,
              readfile.interpret,
              writefile.interpret,
              managefile.interpret),
            close)
        }
    }
}

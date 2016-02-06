/*
 * Copyright 2014 - 2015 SlamData Inc.
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

package quasar.api.services

import quasar.Predef._
import quasar.api._
import quasar.api.ServerOps.StaticContent
import quasar.api.{Destination, HeaderParam}
import quasar.fs._
import quasar.fs.mount.Mounting

import scala.concurrent.duration._
import scala.collection.immutable.ListMap

import org.http4s
import org.http4s.Request
import org.http4s.dsl._
import org.http4s.server.{middleware, HttpService}
import org.http4s.server.middleware.{CORS, GZip}
import org.http4s.server.syntax.ServiceOps
import scalaz._, Scalaz._
import scalaz.concurrent.Task

final case class RestApi(defaultPort: Int, restart: Int => Task[Unit]) {
  import RestApi._

  def httpServices[S[_]: Functor](f: S ~> ResponseOr)
      (implicit
        R: ReadFile.Ops[S],
        W: WriteFile.Ops[S],
        M: ManageFile.Ops[S],
        Q: QueryFile.Ops[S],
        Mnt: Mounting.Ops[S],
        S0: Task :<: S,
        S1: FileSystemFailureF :<: S
      ): Map[String, HttpService] =
    AllServices[S].mapValues(qsvc => withDefaultMiddleware(qsvc.toHttpService(f)))

  def AllServices[S[_]: Functor]
      (implicit
        R: ReadFile.Ops[S],
        W: WriteFile.Ops[S],
        M: ManageFile.Ops[S],
        Q: QueryFile.Ops[S],
        Mnt: Mounting.Ops[S],
        S0: Task :<: S,
        S1: FileSystemFailureF :<: S
      ): ListMap[String, QHttpService[S]] =
    ListMap(
      //"/compile/fs"   -> query.compileService(f),
      "/data/fs"      -> data.service[S],
      "/metadata/fs"  -> metadata.service[S] //,
      //"/mount/fs"     -> mount.service(f),
      //"/query/fs"     -> query.service(f),
      //"/server"       -> server.service(defaultPort, restart),
      //"/welcome"      -> welcome.service
    )
}

object RestApi {
  def withDefaultMiddleware(service: HttpService): HttpService =
    cors(GZip(HeaderParam(service.orElse {
      HttpService {
        case req if req.method == OPTIONS => Ok()
      }
    })))

  def cors(svc: HttpService): HttpService = CORS(
    svc,
    middleware.CORSConfig(
      anyOrigin = true,
      allowCredentials = false,
      maxAge = 20.days.toSeconds,
      allowedMethods = Some(Set("GET", "PUT", "POST", "DELETE", "MOVE", "OPTIONS")),
      allowedHeaders = Some(Set(Destination.name.value)))) // NB: actually needed for POST only
}

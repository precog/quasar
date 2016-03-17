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

package quasar.api.services

import quasar.Predef._
import quasar.api._
import quasar.api.{Destination, HeaderParam}
import quasar.fs._
import quasar.fs.mount._

import scala.concurrent.duration._
import scala.collection.immutable.ListMap

import org.http4s.HttpService
import org.http4s.dsl._
import org.http4s.server.HttpMiddleware
import org.http4s.server.middleware.{CORS, CORSConfig, GZip}
import org.http4s.server.syntax._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object RestApi {
  def coreServices[S[_]: Functor]
      (implicit
        S0: Task :<: S,
        S1: ReadFileF :<: S,
        S2: WriteFileF :<: S,
        S3: ManageFileF :<: S,
        S4: MountingF :<: S,
        S5: QueryFileF :<: S,
        S6: FileSystemFailureF :<: S,
        S7: MountConfigsF :<: S
      ): Map[String, QHttpService[S]] =
    ListMap(
      "/compile/fs"  -> query.compile.service[S],
      "/data/fs"     -> data.service[S],
      "/metadata/fs" -> metadata.service[S],
      "/mount/fs"    -> mount.service[S],
      "/query/fs"    -> query.execute.service[S]
    )

  val additionalServices: Map[String, HttpService] =
    ListMap(
      "/welcome" -> welcome.service
    )

  /** Converts `QHttpService`s into `HttpService`s and mounts them along with
    * any additional `HttpService`s provided, applying the default middleware
    * to the result.
    *
    * TODO: Is using `Prefix` necessary? Can we replace with
    *       `org.http4s.server.Router` instead?
    */
  def finalizeServices[S[_]: Functor](
    f: S ~> ResponseOr)(
    qsvcs: Map[String, QHttpService[S]],
    hsvcs: Map[String, HttpService]
  ): HttpService = {
    val allSvcs = qsvcs.mapValues(_.toHttpService(f)) ++ hsvcs
    // Sort by prefix length so that foldLeft results in routes procesed in
    // descending order (longest first), this ensures that something mounted
    // at `/a/b/c` is consulted before a mount at `/a/b`.
    defaultMiddleware(allSvcs.toList.sortBy(_._1.length).foldLeft(HttpService.empty) {
      case (acc, (path, svc)) => Prefix(path)(svc) orElse acc
    })
  }

  def defaultMiddleware: HttpMiddleware =
    cors <<< gzip <<< HeaderParam <<< passOptions

  val cors: HttpMiddleware =
    CORS(_, CORSConfig(
      anyOrigin = true,
      allowCredentials = false,
      maxAge = 20.days.toSeconds,
      allowedMethods = Some(Set("GET", "PUT", "POST", "DELETE", "MOVE", "OPTIONS")),
      allowedHeaders = Some(Set(Destination.name.value)))) // NB: actually needed for POST only

  val gzip: HttpMiddleware =
    GZip(_)

  val passOptions: HttpMiddleware =
    _ orElse HttpService {
      case req if req.method == OPTIONS => Ok()
    }
}

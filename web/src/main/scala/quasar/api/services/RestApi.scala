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

package quasar.api.services

import slamdata.Predef._
import quasar.api._, ToApiError.ops._, ToQResponse.ops._
import quasar.api.{Destination, HeaderParam}
import quasar.contrib.scalaz.catchable._
import quasar.effect.{ScopeExecution, Timing, TimingRepository}
import quasar.fp.{TaskRef, liftMT}
import quasar.fp.free.foldMapNT
import quasar.fs._
import quasar.fs.mount._
import quasar.fs.mount.module.Module
import quasar.fs.mount.cache.VCache, VCache.{VCacheExpR, VCacheKVS}
import quasar.main.MetaStoreLocation
import quasar.fs.mount.cache.VCache, VCache.VCacheKVS

import scala.concurrent.duration._
import scala.collection.immutable.ListMap

import org.http4s._
import org.http4s.dsl._
import org.http4s.server.HttpMiddleware
import org.http4s.server.middleware.{CORS, CORSConfig, GZip}
import org.http4s.server.syntax._
import org.slf4j.LoggerFactory
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object RestApi {
  def coreServices[S[_], T](executionIdRef: TaskRef[Long], timingRepo: TimingRepository)
      (implicit
        S0: Task :<: S,
        S1: ReadFile :<: S,
        S2: WriteFile :<: S,
        S3: ManageFile :<: S,
        S4: QueryFile :<: S,
        S5: FileSystemFailure :<: S,
        S6: Mounting :<: S,
        S7: MountingFailure :<: S,
        S8: PathMismatchFailure :<: S,
        S9: Module :<: S,
        S10: Module.Failure :<: S,
        S11: Analyze :<: S,
        S12: MetaStoreLocation :<: S,
        S13: VCacheKVS :<: S,
        S15: VCacheExpR :<: S,
        S14: Timing :<: S,
        SE: ScopeExecution[Free[S, ?], T]
      ): Map[String, QHttpService[S]] =
    ListMap(
      "/compile/fs"   -> query.compile.service[S],
      "/estimate/fs"  -> query.analysis.service[S],
      "/data/fs"      -> data.service[S],
      "/metadata/fs"  -> metadata.service[S],
      "/mount/fs"     -> mount.service[S],
      "/query/fs"     -> query.execute.service[S, T](executionIdRef),
      "/invoke/fs"    -> invoke.service[S],
      "/schema/fs"    -> analyze.schema.service[S],
      "/metastore"    -> metastore.service[S],
      "/timings"      -> timings.service[S](timingRepo)
    )

  val additionalServices: Map[String, HttpService] =
    ListMap(
      "/welcome" -> welcome.service
    )

  /** Mount services and apply default middleware to the result.
    *
    * TODO: Replace `Prefix` with `org.http4s.server.Router`.
    */
  def finalizeServices(svcs: Map[String, HttpService]): HttpService = {
    // Sort by prefix length so that foldLeft results in routes processed in
    // descending order (longest first), this ensures that something mounted
    // at `/a/b/c` is consulted before a mount at `/a/b`.
    defaultMiddleware(
      svcs.toList.sortBy(_._1.length).foldLeft(HttpService.empty) {
        case (acc, (path, svc)) => Prefix(path)(svc) orElse acc
      })
  }

  def toHttpServices[S[_]](
    f: S ~> FailedResponseOr,
    svcs: Map[String, QHttpService[S]])
    : Map[String, HttpService] =
    toHttpServicesF(foldMapNT(f), svcs)

  def toHttpServicesF[S[_]](
    f: Free[S, ?] ~> FailedResponseOr,
    svcs: Map[String, QHttpService[S]])
    : Map[String, HttpService] =
    svcs.mapValues(_.toHttpServiceF(f))

  def defaultMiddleware: HttpMiddleware =
    cors                            compose
    gzip                            compose
    RFC5987ContentDispositionRender compose
    HeaderParam                     compose
    passOptions                     compose
    errorHandling                   compose
    requestLogging

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

  val errorHandling: HttpMiddleware =
    _.mapK(_ handle {
      case msgFail: MessageFailure =>
        msgFail.toApiError
          .toResponse[Task]
          .toHttpResponse(liftMT[Task, FailedResponseT])
    })

  val requestLogging: HttpMiddleware =
    RequestLogging(LoggerFactory.getLogger("quasar.api.services.request"), Set.empty)
}

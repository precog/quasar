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
import quasar.api._
import quasar.db.DbConnectionConfig
import quasar.fp._
import quasar.fp.free._
import quasar.fs.FileSystemType
import quasar.fs.mount.{ConnectionUri, MountConfig, MountType}
import quasar.fs.mount.cache.ViewCache
import quasar.main._
import quasar.metastore.{MetaStore, MetaStoreAccess, MetaStoreFixture, PathedMountConfig, PathedViewCache, Schema}
import quasar.metastore.MetaStore.ShouldInitialize
import quasar.sql._
import quasar.Variables

import java.time.Instant

import argonaut._, Argonaut._
import doobie.imports._
import org.http4s._, Status._
import org.http4s.Method.PUT
import org.http4s.syntax.service._
import org.http4s.argonaut._
import pathy.Path.{file, rootDir}
import scalaz._, Scalaz._
import scalaz.concurrent.Task

class MetastoreServiceSpec extends quasar.Qspec {

  def serviceWithMetaStore(persist: DbConnectionConfig => MainTask[Unit] = _ => ().point[MainTask]): (Service[Request, Response], MetaStore) =
    (for {
      ms    <- MetaStoreFixture.createNewTestMetastore()
      inter <- Fixture.inMemFSWeb(persist = persist, metaRefT = TaskRef(ms))
    } yield (quasar.api.services.metastore.service[CoreEffIO].toHttpService(inter).orNotFound, ms)).unsafePerformSync

  def service(persist: DbConnectionConfig => MainTask[Unit] = _ => ().point[MainTask]): Service[Request, Response] =
    serviceWithMetaStore(persist)._1

  "Metastore service" should {
    "return current metastore with password obscured" in {
      val dbConfig = DbConnectionConfig.PostgreSql(
        host = None,
        database = None,
        userName = "bob",
        password = "bobIsAwesome",
        parameters = Map.empty)
      type Eff[A] = Coproduct[Task, MetaStoreLocation, A]
      val inter = liftMT[Task, FailedResponseT] compose (reflNT[Task] :+: MetaStoreLocation.impl.constant(dbConfig))
      val service = quasar.api.services.metastore.service[Eff].toHttpService(inter).orNotFound
      val get = Request()
      val resp = service(get).unsafePerformSync
      resp.as[Json].unsafePerformSync must_===
        Json(
          "postgresql" := Json(
            "userName" := "bob",
            "password" := "****"))
    }
    "succeed in changing metastore without initialize parameter if metastore is already initialized" in {
      val newConn = MetaStoreFixture.createNewTestMetaStoreConfig.unsafePerformSync
      // Connect to it beforehand to initialize it
      val meta = MetaStore.connect(newConn, initializeOrUpdate = ShouldInitialize(true), List(Schema.schema), Nil)
                   .run.unsafePerformSync.valueOr(e => scala.sys.error("Failed to initialize test metastore because: " + e.message))
      meta.shutdown.unsafePerformSync
      val req = Request(method = PUT).withBody(newConn.asJson).unsafePerformSync
      val resp = service()(req).unsafePerformSync
      val expectedUrl = DbConnectionConfig.connectionInfo(newConn).url
      resp.as[String].unsafePerformSync must_=== s"Now using metastore located at $expectedUrl"
      resp.status must_=== Ok
    }
    "succeed in changing the metastore with initialize parameter" in {
      val newConn = MetaStoreFixture.createNewTestMetaStoreConfig.unsafePerformSync
      val req = Request(method = PUT, uri = Uri().+?("initialize")).withBody(newConn.asJson).unsafePerformSync
      val resp = service()(req).unsafePerformSync
      val expectedUrl = DbConnectionConfig.connectionInfo(newConn).url
      resp.as[String].unsafePerformSync must_=== s"Now using newly initialized metastore located at $expectedUrl"
      resp.status must_=== Ok
    }
    "copy metastore" in {
      val f = rootDir </> file("a")
      val pathedMountConfig = PathedMountConfig(
        rootDir </> file("mimir"),
        MountType.fileSystemMount(FileSystemType("local")),
        ConnectionUri("/tmp/local"))
      val (svc, srcMeta) = serviceWithMetaStore()
      val viewCache = ViewCache(
        MountConfig.ViewConfig(sqlB"α", Variables.empty), None, None, 0, None, None,
        0, Instant.ofEpochSecond(0), ViewCache.Status.Pending, None, f, None)
      val pvc = PathedViewCache(f, viewCache)

      (for {
        dstConn <- MetaStoreFixture.createNewTestMetaStoreConfig
        dstMeta <- MetaStore.connect(dstConn, initializeOrUpdate = ShouldInitialize(true), List(Schema.schema), Nil)
                  .run.map(_.valueOr(e => scala.sys.error("Failed to initialize test metastore because: " + e.message)))
        srcTrans = srcMeta.transactor
        dstTrans = dstMeta.transactor
        _     <- MetaStoreAccess.insertPathedMountConfig(pathedMountConfig).transact(srcTrans)
        _     <- MetaStoreAccess.insertViewCache(pvc).transact(srcTrans)
        req   <- Request(method = PUT, uri = Uri() +? ("initialize") +? ("copy")).withBody(dstConn.asJson)
        resp  <- svc(req)
        mnts  <- MetaStoreAccess.mounts.transact(dstTrans)
        vmnts <- MetaStoreAccess.viewCaches.transact(dstTrans)
        expectedUrl <- DbConnectionConfig.connectionInfo(dstConn).url.point[Task]
      } yield {
        resp.status must_=== Ok
        mnts  must_=== List(pathedMountConfig)
        vmnts must_=== List(pvc)
        resp.as[String].unsafePerformSync must_=== s"Metastore copied. Now using newly initialized metastore located at $expectedUrl"
      }).unsafePerformSync
    }
    "persist change to metastore" in {
      val newConn = MetaStoreFixture.createNewTestMetaStoreConfig.unsafePerformSync
      val req = Request(method = PUT, uri = Uri().+?("initialize")).withBody(newConn.asJson).unsafePerformSync
      var persisted: DbConnectionConfig = null
      def persist(db: DbConnectionConfig): MainTask[Unit] = Task.delay{persisted = db}.liftM[MainErrT]
      val resp = service(persist)(req).unsafePerformSync
      "Config was persisted" ==> (persisted must_=== newConn)
    }
    "fail to change metastore with invalid configuration" in {
      val req = Request(method = PUT).withBody(Json("hello" := "there")).unsafePerformSync
      val resp = service()(req).unsafePerformSync
      resp.as[Json].unsafePerformSync must_===
        Json("error" := Json(
          "status" := "UninitializedMetastore",
          "detail" := Json("message" := "unrecognized metastore type: hello; expected 'h2' or 'postgresql'" )))
      resp.status must_=== BadRequest
    }
    "fail to change metastore if metastore is not already initialized (or updated) and initialize parameter was not used" in {
      val newConn = MetaStoreFixture.createNewTestMetaStoreConfig.unsafePerformSync
      val req = Request(method = PUT).withBody(newConn.asJson).unsafePerformSync
      val resp = service()(req).unsafePerformSync
      resp.as[Json].unsafePerformSync must_===
        Json("error" := Json(
          "status" := "UninitializedMetastore",
          "detail" := Json("message" := "MetaStore requires initialization, try running the 'initUpdateMetaStore' command.")))
      resp.status must_=== BadRequest
    }
  }
}

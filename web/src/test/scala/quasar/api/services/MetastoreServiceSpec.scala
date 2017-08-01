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

package quasar.api.services

import slamdata.Predef._
import quasar.api._
import quasar.db.DbConnectionConfig
import quasar.fp._
import quasar.fp.free._
import quasar.main._
import quasar.metastore.MetaStoreFixture

import argonaut._, Argonaut._
import org.http4s._, Status._
import org.http4s.Method.PUT
import org.http4s.argonaut._
import scalaz._, Scalaz._, concurrent.Task

class MetastoreServiceSpec extends quasar.Qspec {

  type Eff[A] = Coproduct[Task, MetaStoreLocation, A]

  val metastore = MetaStoreFixture.createNewTestMetastore.unsafePerformSync
  val metaRef   = TaskRef(metastore).unsafePerformSync

  def inter(persist: DbConnectionConfig => MainTask[Unit]): Eff ~> ResponseOr =
    liftMT[Task, ResponseT] :+:
    (liftMT[Task, ResponseT] compose MetaStoreLocation.impl.default(metaRef, persist))

  def service(persist: DbConnectionConfig => MainTask[Unit] = _ => ().point[MainTask]) =
    quasar.api.services.metastore.service[Eff].toHttpService(inter(persist))

  "Metastore service" should {
    "return current metastore" in {
      val req = Request()
      val resp = service()(req).unsafePerformSync
      resp.as[Json].unsafePerformSync must_=== metastore.connectionInfo.asJson
    }
    "succeed in changing metastore" in {
      val newConn = MetaStoreFixture.createNewTestMetaStoreConfig.unsafePerformSync
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
    "persist change to metastore" in {
      val newConn = MetaStoreFixture.createNewTestMetaStoreConfig.unsafePerformSync
      val req = Request(method = PUT).withBody(newConn.asJson).unsafePerformSync
      var persisted: DbConnectionConfig = null
      def persist(db: DbConnectionConfig): MainTask[Unit] = Task.delay(persisted = db).liftM[MainErrT]
      val resp = service(persist)(req).unsafePerformSync
      persisted must_=== newConn
    }
    "fail to change metastore with invalid configuration" in {
      val req = Request(method = PUT).withBody(Json("hello" := "there")).unsafePerformSync
      val resp = service()(req).unsafePerformSync
      resp.as[String].unsafePerformSync must_=== """{ "error": { "status": "Bad Request", "detail": { "message": "unrecognized metastore type: hello; expected 'h2' or 'postgresql'" } } }"""
      resp.status must_=== BadRequest
    }
  }
}

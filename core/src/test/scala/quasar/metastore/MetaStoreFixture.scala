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

package quasar.metastore

import slamdata.Predef._
import quasar.fp.free.foldMapNT
import quasar.db._

import scala.util.Random.nextInt

import doobie.imports._
import scalaz._, Scalaz._
import scalaz.concurrent.Task
import org.specs2.mutable.SpecificationLike
import org.specs2.specification.AfterAll

trait MetaStoreFixture {
  def schema: Schema[Int]

  def rawTransactor: Transactor[Task]

  // TODO: Check that this is still the case
  // Wish we could use the specs2 trait `BeforeAll` in order to accomplish this, but it does not seem
  // to work in this version (3.7.3-scalacheck-1.12)
  lazy val transactor = {
    schema.updateToLatest.transact(rawTransactor).unsafePerformSync
    rawTransactor
  }

  def interpretIO: ConnectionIO ~> Task = {
    val rback = λ[ConnectionIO ~> ConnectionIO](_ <* HC.rollback)

    transactor.trans compose rback
  }

  def interpretF[S[_]](f: S ~> ConnectionIO): Free[S, ?] ~> Task =
    interpretIO compose foldMapNT(f)
}

object MetaStoreFixture {
  def createNewTestMetaStoreConfig: Task[DbConnectionConfig] =
      Task.delay { DbUtil.inMemoryConfig(s"test_mem_$nextInt") }
  def createNewTestTransactor: Task[Transactor[Task]] =
    createNewTestMetastore.map(_.trans.transactor)
  def createNewTestMetastore: Task[MetaStore] =
    createNewTestMetaStoreConfig.map(testConfig =>
      MetaStore(testConfig, StatefulTransactor(DbUtil.simpleTransactor(DbConnectionConfig.connectionInfo(testConfig)), Task.now(()))))
}

trait H2MetaStoreFixture extends MetaStoreFixture {
  def rawTransactor = DbUtil.simpleTransactor(
    DbConnectionConfig.connectionInfo(DbUtil.inMemoryConfig(s"test_mem_${this.getClass.getSimpleName}")))
}

trait PostgreSqlMetaStoreFixture
    extends MetaStoreFixture
    with    PostgresTxFixture
    with    SpecificationLike
    with    AfterAll {
  // The `toLowerCase` is important here to make sure `doobie` can connect to the database properly
  lazy val transactorOption =
    postgreSqlTransactor(this.getClass.getSimpleName.toLowerCase).run.unsafePerformSync

  args(skipAll = transactorOption.isEmpty)

  private val failMessage = "You must configure the quasar_metastore backend as described in the README " +
                        "in order to run this test without encountering a failure (yes, we know it "      +
                        "would be better if we could tell specs2 to mark it as skipped when that "        +
                        "backend is not present, but that's tricky because of the way doobie "            +
                        "provides us with some fixtures that are inheritance based"

  override def rawTransactor =
    transactorOption.getOrElse(throw new Exception(failMessage)).transactor

  override def afterAll = transactorOption.get.shutdown.unsafePerformSync
}

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
  def createNewTestTransactor(schema: List[Schema[Int]] = List(quasar.metastore.Schema.schema)): Task[Transactor[Task]] =
    createNewTestMetastore(schema).map(_.trans.transactor)
  def createNewTestMetastore(schema: List[Schema[Int]] = List(quasar.metastore.Schema.schema)): Task[MetaStore] =
    for {
      testConfig <- createNewTestMetaStoreConfig
      trans      =  simpleTransactor(DbConnectionConfig.connectionInfo(testConfig))
      _          <- schema.traverse(_.updateToLatest).transact(trans)
    } yield MetaStore(testConfig, StatefulTransactor(trans, Task.now(())), schema)
}

trait H2MetaStoreFixture extends MetaStoreFixture {
  def rawTransactor = simpleTransactor(
    DbConnectionConfig.connectionInfo(DbUtil.inMemoryConfig(s"test_mem_${this.getClass.getSimpleName}")))
}

trait PostgreSqlMetaStoreFixture
    extends MetaStoreFixture
    with    PostgresTxFixture
    with    SpecificationLike {
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
    transactorOption.getOrElse(throw new Exception(failMessage))
}

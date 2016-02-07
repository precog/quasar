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

package quasar.physical.mongodb

import quasar.{EnvironmentError2, rethrow}
import quasar.fp.free._
import quasar.fs._
import quasar.physical.mongodb.fs._
import quasar.regression._

import com.mongodb.ConnectionString
import com.mongodb.async.client.MongoClients
import scalaz._
import scalaz.concurrent.Task

object filesystems {
  def testFileSystem(
    cs: ConnectionString,
    prefix: ADir
  ): Task[FileSystem ~> Task] = for {
    client   <- Task.delay(MongoClients create cs)
    mongofs0 <- rethrow[Task, EnvironmentError2]
                  .apply(mongoDbFileSystem(client, DefaultDb fromPath prefix))
    mongofs  =  rethrow[Task, WorkflowExecutionError] compose mongofs0
  } yield mongofs

  def testFileSystemIO(
    cs: ConnectionString,
    prefix: ADir
  ): Task[FileSystemIO ~> Task] =
    testFileSystem(cs, prefix)
      .map(interpret2(NaturalTransformation.refl[Task], _))
}

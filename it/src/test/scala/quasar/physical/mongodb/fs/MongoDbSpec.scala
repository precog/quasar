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

package quasar.physical.mongodb.fs

import slamdata.Predef._
import quasar._
import quasar.contrib.pathy.ADir
import quasar.fs.FileSystemType
import quasar.fs.mount.ConnectionUri
import quasar.physical.mongodb.Collection

import com.mongodb.async.client.MongoClient
import org.specs2.specification.core.{Fragment, Fragments}
import org.specs2.specification.create.DefaultFragmentFactory._
import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object MongoDbSpec {
  def connect(uri: ConnectionUri): Task[MongoClient] =
    asyncClientDef[Task](uri).run.foldMap(NaturalTransformation.refl).flatMap(_.fold(
      err => Task.fail(new RuntimeException(err.toString)),
      Task.now))

  def tempColl(prefix: ADir): Task[Collection] =
    NameGenerator.salt >>= (n =>
      Collection.fromFile(prefix </> file(n)).fold(
        err => Task.fail(new RuntimeException(err.shows)),
        Task.now))

  def clientShould(fsType: FileSystemType)(examples: (BackendName, ADir, MongoClient, MongoClient) => Fragment): Fragments =
    TestConfig.testDataPrefix.flatMap { prefix =>
      TestConfig.fileSystemConfigs(fsType) >>= (_.traverse { case (ref, setupUri, testUri) =>
        (connect(setupUri) |@| connect(testUri))((setupClient, testClient) =>
          Fragments(
            examples(ref.name, prefix, setupClient, testClient),
            step(testClient.close),
            step(setupClient.close)))
      })
    }.map(_.suml).unsafePerformSync
}

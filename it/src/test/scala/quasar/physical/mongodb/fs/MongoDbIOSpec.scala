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
import quasar.physical.mongodb._

import scalaz._, Scalaz._

class MongoDbIOSpec extends QuasarSpecification {
  import MongoDbSpec._

  clientShould(FsType) { (backend, prefix, setupClient, testClient) =>
    import MongoDbIO._

    backend.name should {
      "get mongo version" in {
        implicit val ord: scala.math.Ordering[ServerVersion] = Order[ServerVersion].toScalaOrdering

        serverVersion.run(testClient).unsafePerformSync must beGreaterThan(ServerVersion(2, 6, None, ""))
      }

      "get stats" in {
        (for {
          coll  <- tempColl(prefix)
          _     <- insert(
                    coll,
                    List(Bson.Doc(ListMap("a" -> Bson.Int32(0)))).map(_.repr)).run(setupClient)
          stats <- collectionStatistics(coll).run(testClient)
          _     <- dropCollection(coll).run(setupClient)
        } yield {
          stats.count    must_=== 1
          stats.dataSize must beGreaterThan(0L)
          stats.sharded  must beFalse
        }).unsafePerformSync
      }

      "get the default index on _id" in {
        (for {
          coll <- tempColl(prefix)
          _    <- insert(
                    coll,
                    List(Bson.Doc(ListMap("a" -> Bson.Int32(0)))).map(_.repr)).run(setupClient)
          idxs <- indexes(coll).run(testClient)
          _    <- dropCollection(coll).run(setupClient)
        } yield {
          // NB: this index is not marked "unique", counter-intuitively
          idxs must_=== Set(Index("_id_", NonEmptyList(BsonField.Name("_id") -> IndexType.Ascending), false))
        }).unsafePerformSync
      }
    }
  }
}

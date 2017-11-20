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

package quasar.physical.mongodb

import slamdata.Predef._
import quasar.physical.mongodb.expression._
import quasar.physical.mongodb.workflow._
import quasar.sql._

import eu.timepit.refined.auto._
import org.bson.{BsonDocument, BsonDouble}
import scalaz._

class PlannerSigilSpec extends
    PlannerHelpers {

  import Reshape.reshape
  import CollectionUtil._

  import fixExprOp._
  import PlannerHelpers._

  "sigil detection" should {
    "project away root-level sigil" >> {
      val getDoc: Collection => OptionT[EitherWriter, BsonDocument] =
        _ => OptionT.some(new BsonDocument(
          sigil.Quasar,
          new BsonDocument("bar", new BsonDouble(4.2))))

      plan3_4(sqlE"select bar, baz from foo", defaultStats, defaultIndexes, getDoc) must
        beWorkflow(
          chain[Workflow](
            $read(collection("db", "foo")),
            $project(
              reshape(
                "bar" -> $field(sigil.Quasar, "bar"),
                "baz" -> $field(sigil.Quasar, "baz")),
              ExcludeId)))
    }

    "project away sigil nested in map-reduce result" >> {
      val getDoc: Collection => OptionT[EitherWriter, BsonDocument] =
        _ => OptionT.some(
          new BsonDocument(
            sigil.Value,
            new BsonDocument(
              sigil.Quasar,
              new BsonDocument("bar", new BsonDouble(4.2)))))

      plan3_4(sqlE"select bar, baz from foo", defaultStats, defaultIndexes, getDoc) must
        beWorkflow(
          chain[Workflow](
            $read(collection("db", "foo")),
            $project(
              reshape(
                "bar" -> $field(sigil.Value, sigil.Quasar, "bar"),
                "baz" -> $field(sigil.Value, sigil.Quasar, "baz")),
              ExcludeId)))
    }
  }
}

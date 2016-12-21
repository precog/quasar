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

package quasar.sql

import quasar.Predef._

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import scalaz._

class SQLSpec extends quasar.Qspec {

  implicit def stringToQuery(s: String): Query = Query(s)

  "namedProjections" should {
    "create unique names" >> {
      "when two fields have the same name" in {
        val query = "SELECT owner.name, car.name from owners as owner join cars as car on car._id = owner.carId"
        val projections = fixParser.parse(query).toOption.get.project.asInstanceOf[Select[Fix[Sql]]].projections
        projectionNames(projections, None) must beLike { case \/-(list) =>
          list.map(_._1) must contain(allOf("name", "name0"))
        }
      }
      "when a field and an alias have the same name" in {
        val query = "SELECT owner.name, car.model as name from owners as owner join cars as car on car._id = owner.carId"
        val projections = fixParser.parse(query).toOption.get.project.asInstanceOf[Select[Fix[Sql]]].projections
        projectionNames(projections, None) must beLike { case \/-(list) =>
          list.map(_._1) must contain(allOf("name0", "name"))
        }
      }
    }
  }
}

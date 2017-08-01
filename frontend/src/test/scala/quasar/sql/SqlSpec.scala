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

package quasar.sql

import slamdata.Predef._

import matryoshka._
import matryoshka.data.Fix
import matryoshka.implicits._
import scalaz._, Scalaz._

class SQLSpec extends quasar.Qspec {

  "namedProjections" should {
    "create unique names" >> {
      "when two fields have the same name" in {
        val query = sqlE"SELECT owner.name, car.name from owners as owner join cars as car on car.`_id` = owner.carId"
        val projections = query.project.asInstanceOf[Select[Fix[Sql]]].projections
        projectionNames(projections, None) must beLike { case \/-(list) =>
          list.map(_._1) must contain(allOf("name", "name0"))
        }
      }
      "when a field and an alias have the same name" in {
        val query = sqlE"SELECT owner.name, car.model as name from owners as owner join cars as car on car.`_id` = owner.carId"
        val projections = query.project.asInstanceOf[Select[Fix[Sql]]].projections
        projectionNames(projections, None) must beLike { case \/-(list) =>
          list.map(_._1) must contain(allOf("name0", "name"))
        }
      }
    }
  }
  "apply arguments to a function declaration" >> {
    "in the general case" in {
      val funcDef = FunctionDecl(CIName("foo"), args = List(CIName("bar")), body = sqlE"SELECT * from `/person` where :bar")
      funcDef.applyArgs(List(sqlE"age > 10")) must_=== sqlE"SELECT * from `/person` where age > 10".right
    }
    "if the variable is inside of a `from`" in {
      val funcDef = FunctionDecl(CIName("foo"), args = List(CIName("bar")), body = sqlE"SELECT * from :bar")
      funcDef.applyArgs(List(sqlE"`/person`")) must_=== sqlE"SELECT * from `/person`".right
    }
  }
}

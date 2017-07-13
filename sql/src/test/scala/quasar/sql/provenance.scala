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
import quasar.sql.SemanticAnalysis._, Provenance._

import pathy.Path._

class ProvenanceSpec extends quasar.Qspec {
  val Rel1 = Relation(TableRelationAST(file("foo"), None))
  val Rel2 = Relation(TableRelationAST(file("bar"), None))

  "flatten" should {
    "flatten singleton in" in {
      Value.flatten must_=== (Set(Value))
    }

    "flatten 2-way product in" in {
      (Value & Empty).flatten must_=== (Set(Value, Empty))
    }

    "flatten 3-way product in" in {
      (Value & Empty & Rel1).flatten must_=== (Set(Value, Empty, Rel1))
    }

    "flatten 2-way coproduct in" in {
      (Value | Empty).flatten must_=== (Set(Value, Empty))
    }

    "flatten 3-way coproduct in" in {
      (Value | Empty | Rel1).flatten must_=== (Set(Value, Empty, Rel1))
    }
  }

  "simplify" should {
    "simplify 3-way twice duplicated coproduct" in {
      (Value | Value | Empty | Empty | Rel1 | Rel1).simplify.flatten must_=== (Set(Value, Rel1))
    }
  }

  "equality" should {
    "be order independent for 3-way products" in {
      (Value & Rel1 & Rel2) must_=== (Rel1 & Value & Rel2)
    }
  }
}

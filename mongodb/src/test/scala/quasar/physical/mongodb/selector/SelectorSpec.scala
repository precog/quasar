/*
 * Copyright 2014–2018 SlamData Inc.
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

package quasar.physical.mongodb.selector

import slamdata.Predef._

import quasar.physical.mongodb.{Bson, BsonField}

class SelectorSpec extends quasar.Qspec  {

  implicit def toBson(x: Int) = Bson.Int32(x)
  implicit def toField(name: String) = BsonField.Name(name)

  "Selector" should {
    import Selector._

    "bson" should {
      "render simple expr" in {
        CExpr(Lt(10)).bson must_== Bson.Doc("$lt" -> Bson.Int32(10))
      }

      "render $not expr" in {
        NotCExpr(Lt(10)).bson must_== Bson.Doc(ListMap("$not" -> Bson.Doc("$lt" -> Bson.Int32(10))))
      }

      "render simple selector" in {
        val sel = Doc(BsonField.Name("foo") -> Gt(10))

        sel.bson must_== Bson.Doc(ListMap("foo" -> Bson.Doc("$gt" -> Bson.Int32(10))))
      }

      "render simple selector with path" in {
        val sel = Doc(
          BsonField.Name("foo") \ BsonField.Name("3") \ BsonField.Name("bar") -> Gt(10)
        )

        sel.bson must_== Bson.Doc("foo.3.bar" -> Bson.Doc("$gt" -> Bson.Int32(10)))
      }

      "render flattened $and" in {
        val cs = And(
          Doc(BsonField.Name("foo") -> Gt(10)),
          And(
            Doc(BsonField.Name("foo") -> Lt(20)),
            Doc(BsonField.Name("foo") -> Neq(15))
          )
        )
        cs.bson must_==
          Bson.Doc("$and" -> Bson.Arr(
            Bson.Doc("foo" -> Bson.Doc("$gt" -> Bson.Int32(10))),
            Bson.Doc("foo" -> Bson.Doc("$lt" -> Bson.Int32(20))),
            Bson.Doc("foo" -> Bson.Doc("$ne" -> Bson.Int32(15)))))
      }

      "render not(eq(...))" in {
        val cond = NotCExpr(Eq(10))
        cond.bson must_== Bson.Doc("$ne" -> Bson.Int32(10))
      }

      "render nested And within Or" in {
        val cs =
          Or(
            And(
              Doc(BsonField.Name("foo") -> Gt(10)),
              Doc(BsonField.Name("foo") -> Lt(20))),
            And(
              Doc(BsonField.Name("bar") -> Gte(1)),
              Doc(BsonField.Name("bar") -> Lte(5))))

        cs.bson must_== Bson.Doc("$or" -> Bson.Arr(
          Bson.Doc("$and" -> Bson.Arr(
            Bson.Doc("foo" -> Bson.Doc("$gt" -> Bson.Int32(10))),
            Bson.Doc("foo" -> Bson.Doc("$lt" -> Bson.Int32(20))))),
          Bson.Doc("$and" -> Bson.Arr(
            Bson.Doc("bar" -> Bson.Doc("$gte" -> Bson.Int32(1))),
            Bson.Doc("bar" -> Bson.Doc("$lte" -> Bson.Int32(5)))))))
      }
    }

    "negate" should {
      "rewrite singleton Doc" in {
        val sel = Doc(
          BsonField.Name("x") -> Lt(10),
          BsonField.Name("y") -> Gt(10))
        sel.negate must_== Or(
          Doc(ListMap(("x": BsonField) -> NotCExpr(Lt(10)))),
          Doc(ListMap(("y": BsonField) -> NotCExpr(Gt(10)))))
      }

      "rewrite And" in {
        val sel =
          And(
            Doc(BsonField.Name("x") -> Lt(10)),
            Doc(BsonField.Name("y") -> Gt(10)))
        sel.negate must_== Or(
          Doc(ListMap(("x": BsonField) -> NotCExpr(Lt(10)))),
          Doc(ListMap(("y": BsonField) -> NotCExpr(Gt(10)))))
      }

      "rewrite Doc" in {
        val sel = Doc(
          BsonField.Name("x") -> Lt(10),
          BsonField.Name("y") -> Gt(10))
        sel.negate must_== Or(
          Doc(ListMap(("x": BsonField) -> NotCExpr(Lt(10)))),
          Doc(ListMap(("y": BsonField) -> NotCExpr(Gt(10)))))
      }
    }
  }
}

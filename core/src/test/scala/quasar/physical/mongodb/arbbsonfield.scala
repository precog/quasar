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

import org.scalacheck._

trait ArbBsonField {
  lazy val genBsonFieldName = for {
    c   <- Gen.alphaChar
    str <- Gen.alphaStr
  } yield BsonField.Name(c.toString + str)

  lazy val genBsonFieldIndex = for {
    index <- Gen.chooseNum(0, 10)
  } yield BsonField.Name(index.toString)

  implicit val arbBsonField: Arbitrary[BsonField] = Arbitrary(for {
    list <- Gen.nonEmptyListOf(Gen.oneOf(genBsonFieldName, genBsonFieldIndex))

    f = BsonField(list)

    if (!f.isEmpty)
  } yield f.get)
}

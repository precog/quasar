/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.common.data

import slamdata.Predef._
import quasar.pkg.tests._

trait RValueGenerators {
  import RValueGenerators._

  implicit val rValueArbitray: Arbitrary[RValue] =
    Arbitrary(genRValue(maxDepthDefault))
}

object RValueGenerators {
  val maxDepthDefault: Int = 4
  val atomic: Gen[RValue] = CValueGenerators.genCValue

  private def genRArray(depth: Int): Gen[RArray] =
    genRValue(depth).list.map(RArray(_))

  private def genRObject(depth: Int): Gen[RObject] =
    (genUnicodeString, genRValue(depth)).zip.list.map(vs => RObject(vs.toMap))

  private def genRMeta(depth: Int): Gen[RMeta] = for {
    value <- genRValue(depth)
    meta <- genRValue(depth)
  } yield RMeta(value, RObject(Map(QDataRValue.metaKey -> meta)))

  private def genNestedRValue(depth: Int): Gen[RValue] =
    Gen.oneOf[RValue](
      genRArray(depth),
      genRObject(depth),
      genRMeta(depth)
    )

  def genRValue(depth: Int): Gen[RValue] =
    genNested[RValue](maxDepthDefault, depth, atomic, genNestedRValue(depth - 1))
}

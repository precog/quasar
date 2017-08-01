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

package quasar.yggdrasil
package table

import quasar.yggdrasil.bytecode._
import quasar.precog.common._

import quasar.blueeyes._, json._
import scalaz._, Scalaz._
import quasar.precog.TestSupport._

trait PartitionMergeSpec[M[+_]] extends ColumnarTableModuleTestSupport[M] with SpecificationLike with ScalaCheck {
  import trans._

  def testPartitionMerge = {
    val JArray(elements) = JParser.parseUnsafe("""[
      { "key": [0], "value": { "a": "0a" } },
      { "key": [1], "value": { "a": "1a" } },
      { "key": [1], "value": { "a": "1b" } },
      { "key": [1], "value": { "a": "1c" } },
      { "key": [2], "value": { "a": "2a" } },
      { "key": [3], "value": { "a": "3a" } },
      { "key": [3], "value": { "a": "3b" } },
      { "key": [4], "value": { "a": "4a" } }
    ]""")

    val tbl = fromJson(elements.toStream)

    val JArray(expected) = JParser.parseUnsafe("""[
      "0a",
      "1a;1b;1c",
      "2a",
      "3a;3b",
      "4a"
    ]""")

    val result: M[Table] = tbl.partitionMerge(DerefObjectStatic(Leaf(Source), CPathField("key"))) { table =>
      val reducer = new Reducer[String] {
        def reduce(schema: CSchema, range: Range): String = {
          schema.columns(JTextT).head match {
            case col: StrColumn => range map col mkString ";"
            case _              => abort("Not a StrColumn")
          }
        }
      }

      val derefed = table.transform(DerefObjectStatic(DerefObjectStatic(Leaf(Source), CPathField("value")), CPathField("a")))

      derefed.reduce(reducer).map(s => Table.constString(Set(s)))
    }

    result.flatMap(_.toJson).copoint must_== expected.toStream
  }

}



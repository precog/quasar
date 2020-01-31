/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.qsu
package minimizers

import slamdata.Predef._

object RelationSpec extends quasar.Qspec {
  "transitive closure" should {
    "at least include self" >> {
      Relation.Empty.closure('foo) must_=== Set('foo)
    }

    "include all transitive nodes" >> {
      val r = Relation.Empty +
        ('a -> 'b) +
        ('b -> 'c) +
        ('b -> 'd) +
        ('e -> 'f) +
        ('f -> 'g) +
        ('g -> 'h)

      r.closure('a) must_=== Set('a, 'b, 'c, 'd)
      r.closure('e) must_=== Set('e, 'f, 'g, 'h)
    }
  }
}

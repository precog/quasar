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

package quasar.physical.couchbase.planner

import quasar.NameGenerator
import quasar.physical.couchbase._
import quasar.qscript._

import matryoshka._
import scalaz._

// Join document by field that is not a primary key?
// ╰─ http://stackoverflow.com/a/39264955/343274
// ╰─ https://forums.couchbase.com/t/n1ql-joins-on-document-fields/8418/3

// When falling back to Map/Reduce can quickly arrive at "error (reduction too large)"
// ╰─ https://github.com/couchbase/couchstore/search?utf8=%E2%9C%93&q=MAX_REDUCTION_SIZE

final class EquiJoinPlanner[T[_[_]]: BirecursiveT: ShowT, F[_]: Monad: NameGenerator]
  extends Planner[T, F, EquiJoin[T, ?]] {

  def plan: AlgebraM[M, EquiJoin[T, ?], T[N1QL]] = {
    case EquiJoin(src, lBranch, rBranch, lKey, rKey, f, combine) => unimplementedP("EquiJoin")
  }
}

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

package quasar.physical.marklogic.qscript

import quasar.Data
import quasar.ejson._
import quasar.physical.marklogic.xquery._
import quasar.contrib.iota.copkTraverse

import matryoshka._
import matryoshka.implicits._
import scalaz._

object EJsonPlanner {
  def plan[J, M[_]: Monad, FMT](ejs: J)(
    implicit SP: StructuralPlanner[M, FMT],
             R:  Recursive.Aux[J, EJson]
  ): M[XQuery] = DataPlanner[M, FMT](ejs.cata(Data.fromEJson))
}

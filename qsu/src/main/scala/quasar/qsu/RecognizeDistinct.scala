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

package quasar.qsu

import quasar.fp._
import quasar.qscript.ReduceFuncs
import quasar.qsu.{QScriptUniform => QSU}

import matryoshka.BirecursiveT
import scalaz.syntax.equal._

object RecognizeDistinct {
  import QSUGraph.Extractors._

  // pattern from compileDistinct in compiler.scala
  // TODO this is dumb; we shouldn't be replicating patterns like this
  def apply[T[_[_]]: BirecursiveT](qgraph: QSUGraph[T]): QSUGraph[T] =
    qgraph rewrite {
      case qgraph @ LPReduce(GroupBy(orig1, orig2), ReduceFuncs.Arbitrary(()))
          if orig1.root === orig2.root =>
        qgraph.overwriteAtRoot(QSU.Distinct(orig1.root))
    }
}

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

package quasar.qscript.qsu

import quasar.qscript.FreeMapA
import quasar.qscript.qsu.{QScriptUniform => QSU}

import matryoshka.BirecursiveT
import scalaz.Free
import scalaz.syntax.applicative._

object EliminateUnary {
  import QSUGraph.Extractors._

  def apply[T[_[_]]: BirecursiveT](qgraph: QSUGraph[T]): QSUGraph[T] =
    qgraph rewrite {
      case qgraph @ Unary(source, mf) =>
        qgraph.overwriteAtRoot(QSU.Map(source.root, Free.roll(mf.map(_.point[FreeMapA[T, ?]]))))
    }
}

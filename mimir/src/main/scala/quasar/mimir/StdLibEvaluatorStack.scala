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

package quasar.mimir

import quasar.yggdrasil.table.cf
import scalaz._

trait StdLibEvaluatorStack[M[+ _]]
    extends EvaluatorModule[M]
    with StdLibModule[M]
    with StdLibOpFinderModule[M]
    with StdLibStaticInlinerModule[M]
    with ReductionFinderModule[M]
    with JoinOptimizerModule[M]
    with PredicatePullupsModule[M] {

  trait Lib extends StdLib with StdLibOpFinder
  object library extends Lib

  abstract class Evaluator[N[+ _]](N0: Monad[N])(implicit mn: M ~> N, nm: N ~> M)
      extends EvaluatorLike[N](N0)(mn, nm)
      with StdLibOpFinder
      with StdLibStaticInliner {

    val Exists = library.Exists
    val Forall = library.Forall
    def concatString = library.Infix.concatString.f2
    def coerceToDouble = cf.util.CoerceToDouble
  }
}

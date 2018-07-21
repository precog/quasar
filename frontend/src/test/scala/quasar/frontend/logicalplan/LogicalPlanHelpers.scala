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

package quasar.frontend.logicalplan

import slamdata.Predef._
import quasar.common.data.Data
import quasar.contrib.pathy.sandboxCurrent
import quasar.fp.ski._
import quasar.std.StdLib.structural.MakeMapN

import matryoshka.data.Fix
import matryoshka.implicits._
import pathy.Path._
import scalaz._, Scalaz._

trait LogicalPlanHelpers extends TermLogicalPlanMatchers {
  val optimizer = new Optimizer[Fix[LogicalPlan]]

  val lpf = new LogicalPlanR[Fix[LogicalPlan]]

  val lpr = optimizer.lpr

  def read(file: String): Fix[LogicalPlan] =
    lpf.read(sandboxCurrent(posixCodec.parsePath(Some(_), Some(_), κ(None), κ(None))(file).get).get)

  def makeObj(ts: (String, Fix[LogicalPlan])*): Fix[LogicalPlan] =
    MakeMapN(ts.map(t => lpf.constant(Data.Str(t._1)) -> t._2): _*).embed
}

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

package quasar.physical.marklogic.xquery

import quasar.physical.marklogic.xquery.syntax._

import eu.timepit.refined.auto._
import scalaz.Functor

object mem {
  val m = module("mem", "http://xqdev.com/in-mem-update", "/MarkLogic/appservices/utils/in-mem-update.xqy")

  def nodeDelete[F[_]: Functor: PrologW](node: XQuery): F[XQuery] =
    m("node-delete") apply node

  def nodeInsertChild[F[_]: Functor: PrologW](node: XQuery, child: XQuery): F[XQuery] =
    m("node-insert-child") apply (node, child)
}

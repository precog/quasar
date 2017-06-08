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

package quasar.physical.marklogic.xquery

import quasar.physical.marklogic.xquery.syntax._

import eu.timepit.refined.auto._
import scalaz.Functor

object admin {
  val m = module("admin", "http://marklogic.com/xdmp/admin", "/MarkLogic/admin.xqy")

  def getConfiguration[F[_]: Functor: PrologW]: F[XQuery] =
    m("get-configuration").apply()

  def databaseGetDirectoryCreation[F[_]: Functor: PrologW](config: XQuery, databaseId: XQuery): F[XQuery] =
    m("database-get-directory-creation") apply (config, databaseId)

  def databaseGetUriLexicon[F[_]: Functor: PrologW](config: XQuery, databaseId: XQuery): F[XQuery] =
    m("database-get-uri-lexicon") apply (config, databaseId)
}

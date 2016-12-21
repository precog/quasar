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

package quasar.internal

import quasar._, Predef._
import matryoshka.data.Fix
import matryoshka.implicits._
import quasar.sql.{nullLiteral, Sql}

/** This is used in both web and it tests.
 *  If it's placed in the web test code, then it has to
 *  depend on web's test configuration - which makes the
 *  dependency chain linear. Placing it here allows
 *  it to be independent of web.
 *
 *  TODO:
 *   - move to MountConfig
 *   - don't explode on error, e.g. return SomeError \/ (Fix[Sql], Variables) instead
 *   - implement as viewConfigFromQuery, and call it from viewConfigFromUri
 */
object MountServiceConfig {
  def unsafeViewCfg(q: String, vars: (String, String)*): (Fix[Sql], Variables) =
    (
      sql.fixParser.parse(sql.Query(q)).toOption.fold[Fix[sql.Sql]](nullLiteral[Fix[Sql]]().embed)(x => x),
      Variables(Map(vars.map { case (n, v) => quasar.VarName(n) -> quasar.VarValue(v) }: _*))
    )
}

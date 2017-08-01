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

package quasar.sql

import slamdata.Predef._
import quasar.contrib.contextual.StaticInterpolator

import matryoshka.data.Fix
import scalaz._

object SqlInterpolator {

  object Expr extends StaticInterpolator[Fix[Sql]] {
    def parse(s: String): String \/ Fix[Sql] =
      parser[Fix].parseExpr(Query(s)).leftMap(parseError => s"Not a valid SQL expression: $parseError")
  }

  object ScopedExpr extends StaticInterpolator[ScopedExpr[Fix[Sql]]] {
    def parse(s: String): String \/ ScopedExpr[Fix[Sql]] =
      parser[Fix].parseScopedExpr(s).leftMap(parseError => s"Not a valid SQL scopedExpr: $parseError")
  }

  object Module extends StaticInterpolator[List[Statement[Fix[Sql]]]] {
    def parse(s: String): String \/ List[Statement[Fix[Sql]]] =
      parser[Fix].parseModule(s).leftMap(parseError => s"Not a valid SQL module: $parseError")
  }

}
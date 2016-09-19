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

import quasar.Predef._

import monocle.macros.Lenses
import scalaz.std.string._
import scalaz.syntax.foldable._
import scalaz.syntax.std.option._

// TODO: Possibly introduce `Module` and just have this be a constructor.
@Lenses
final case class MainModule(version: Version, prologs: Prologs, queryBody: XQuery) {
  def render: String = {
    val (funcs, decls) = prologs.partition(Prolog.funcDecl.isMatching)

    val declBlock = decls.toIList.map(d => s"${d.render}${Prolog.Separator}").toNel map { ls =>
      "\n\n" + ls.intercalate("\n")
    }

    val funcBlock = funcs.toIList.map(f => s"${f.render}${Prolog.Separator}").toNel map { ls =>
      "\n\n" + ls.intercalate("\n\n")
    }

    s"${version.render}${Prolog.Separator}${~declBlock}${~funcBlock}\n\n${queryBody}"
  }
}

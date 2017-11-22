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

package quasar.physical.rdbms.fs.postgres

import slamdata.Predef._
import quasar.fp.numeric._
import quasar.physical.rdbms.common.TablePath
import quasar.physical.rdbms.fs.RdbmsScanTable

import doobie.syntax.string._
import doobie.util.fragment.Fragment
import eu.timepit.refined.auto._
import scalaz._
import Scalaz._

trait PostgresScanTable extends RdbmsScanTable {

   def limitFr(limit: Option[Positive]): Fragment = {
     ~limit.map { l =>
       fr"LIMIT" ++ Fragment.const(l.shows)
     }
   }

  def offsetFr(offset: Natural): Fragment = {
    if (offset > 0)
      fr"OFFSET" ++ Fragment.const(offset.shows)
    else Fragment.empty
  }

  override def selectAllQuery(tablePath: TablePath, offset: Natural, limit: Option[Positive]): Fragment = {
    fr"select row_to_json(row) from" ++ Fragment.const(tablePath.shows) ++ fr"row" ++ limitFr(limit) ++ offsetFr(offset)
  }

}

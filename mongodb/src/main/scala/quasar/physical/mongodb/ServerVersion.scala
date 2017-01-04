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

package quasar.physical.mongodb

import quasar.Predef._

import scalaz._, Scalaz._

final case class ServerVersion(major: Int, minor: Int, revision: Option[Int], extra: String)
object ServerVersion {
  val MongoDb2_6 = ServerVersion(2, 6, None, "")
  val MongoDb3_0 = ServerVersion(3, 0, None, "")
  val MongoDb3_2 = ServerVersion(3, 2, None, "")

  private val Pattern = """(?s)(\d+)\.(\d+)(?:\.(\d+))?[-. _]?(.*)""".r

  def fromString(str: String): String \/ ServerVersion = str match {
    case Pattern(major, minor, revision, extra) =>
      ServerVersion(major.toInt, minor.toInt, Option(revision).map(_.toInt), extra).right
    case _ =>
      s"Unable to parse server version: $str".left
  }

  implicit val show: Show[ServerVersion] = Show.show { v =>
    v.major.toString + "." + v.minor.toString +
      v.revision.foldMap("." + _.shows) +
      (if (v.extra.isEmpty) "" else "-" + v.extra)
  }

  implicit val order: Order[ServerVersion] =
    Order.orderBy(v => (v.major, v.minor, v.revision, v.extra))
}

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

package quasar.repl2

import slamdata.Predef._
import quasar.api.{ResourceName, ResourcePath}

import scalaz.{IList, Scalaz}, Scalaz._

sealed trait ReplPath

object ReplPath {
  final case class Relative(path: ResourcePath) extends ReplPath
  final case class Absolute(path: ResourcePath) extends ReplPath

  def unapply(s: String): Option[ReplPath] =
    Option(s).map(parseReplPath)

  private def parseReplPath(s: String): ReplPath =
    if (s.startsWith("/")) Absolute(parseResourcePath(s.drop(1)))
    else Relative(parseResourcePath(s))

  private def parseResourceNames(s: String): IList[ResourceName] =
    if (s === "") IList.empty[ResourceName]
    else IList.fromList(s.split("/").toList).map(ResourceName(_))

  private def parseResourcePath(s: String): ResourcePath =
    ResourcePath.resourceNamesIso(parseResourceNames(s))
}

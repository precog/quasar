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

package quasar.physical.marklogic

import slamdata.Predef._
import quasar.RenderTree

import eu.timepit.refined.api.Refined
import eu.timepit.refined.scalaz._
import eu.timepit.refined.string.{Uri => RUri}
import scalaz.std.string._
import _root_.xml.name.QName

package object cts {
  type Uri = String Refined RUri

  implicit def uriRenderTree: RenderTree[Uri] =
    RenderTree.fromShow[Uri]("Uri")

  implicit def qNameRenderTree: RenderTree[QName] =
    RenderTree.fromShow[QName]("QName")
}

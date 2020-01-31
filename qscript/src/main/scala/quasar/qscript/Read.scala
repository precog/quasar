/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.qscript

import slamdata.Predef.List
import quasar.{IdStatus, RenderTree}

import monocle.macros.Lenses
import scalaz.{Equal, Show}
import scalaz.std.tuple._
import scalaz.syntax.show._
import scalaz.syntax.std.option._

/** A backend-resolved `Root`, which is now a path. */
@Lenses final case class Read[A](path: A, idStatus: IdStatus)

object Read {
  implicit def equal[A: Equal]: Equal[Read[A]] =
    Equal.equalBy((sr => (sr.path, sr.idStatus)))

  implicit def show[A: Show]: Show[Read[A]] = RenderTree.toShow

  implicit def renderTree[A: Show]: RenderTree[Read[A]] =
    RenderTree.simple(List("Read"), r => {
      (r.path.shows + ", " + r.idStatus.shows).some
    })
}

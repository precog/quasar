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

package quasar.db

import slamdata.Predef._

import doobie.imports._
import scala.collection.immutable.SortedMap
import scalaz._, Scalaz._

/**
  * @param updates a sequence of update operations which can be applied in
  * sequence to bring the schema from any previous version up to the latest
  * version, migrating existing data in the process.
  */
// TODO: expose the individual operation of `updates` as List[Update0], so
// they could be `check`ed, etc.? That would restrict what updates are
// allowed to do, though.
final case class Schema[A: Atom: Order](
  readVersion: ConnectionIO[Option[A]],
  writeVersion: A => ConnectionIO[Unit],
  updates: SortedMap[A, ConnectionIO[Unit]]) {

  /** Most recent known version, to which the schema will be updated. */
  def latestVersion: A = updates.lastKey

  /** Compare the schema version found in the DB with the latest version,
    * and return a tuple showing the update to be applied, if needed.
    */
  def updateRequired: ConnectionIO[Option[(Option[A], A)]] =
    readVersion.flatMap(_.cata(
      ver =>
        if (ver < latestVersion) (ver.some, latestVersion).some.point[ConnectionIO]
        else if (ver === latestVersion) none.point[ConnectionIO]
        else connFail(s"unexpected schema version; found: $ver; latest known: $latestVersion"),
      (none[A], latestVersion).some.point[ConnectionIO]))

  def updateToLatest: ConnectionIO[Unit] =
    updateRequired.flatMap(_.cata(
      { case (first, _) =>
        updates.collect {
          case (ver, upd) if first.cata(_ < ver, true) =>
            upd *> writeVersion(ver) *> HC.commit
        }.toList.sequence_[ConnectionIO, Unit]
      },
      ().point[ConnectionIO]))
}

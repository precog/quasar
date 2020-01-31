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

package quasar.impl.cluster

import slamdata.Predef._
import java.net.InetAddress

abstract class Membership[F[_], Id] {
  def localId: F[Id]
  def peers: F[Set[Id]]
  def sample: F[Set[Id]]
  def random: F[Option[Id]]
  def byAddress(addr: InetAddress, port: Int): F[Option[Id]]
}

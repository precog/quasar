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

package quasar.effect

import slamdata.Predef._
import quasar.contrib.scalaz.MonadReader_

import java.util.UUID

import com.fasterxml.uuid._
import scalaz.{:<:, ~>}
import scalaz.std.anyVal._
import scalaz.syntax.equal._

object uuid {
  type UuidReader[F[_]] = MonadReader_[F, UUID]

  object UuidReader {
    def apply[F[_]](implicit F: UuidReader[F]): UuidReader[F] = F
  }

  type GenUUID[A] = Read[UUID, A]

  object GenUUID {
    def Ops[S[_]](implicit S: GenUUID :<: S) =
      Read.Ops[UUID, S]

    def type1[F[_]: Capture]: F[GenUUID ~> F] =
      Capture[F].capture(
        fromNoArg[F](Option(EthernetAddress.fromInterface).fold(
          Generators.timeBasedGenerator)(
          Generators.timeBasedGenerator)))

    def type4[F[_]: Capture]: F[GenUUID ~> F] =
      Capture[F].capture(fromNoArg[F](Generators.randomBasedGenerator))

    ////

    private def fromNoArg[F[_]: Capture](noArgGen: NoArgGenerator): GenUUID ~> F =
      λ[GenUUID ~> F] {
        case Read.Ask(f) => Capture[F].capture(f(noArgGen.generate))
      }
  }

  /** Returns an opaque string from the given UUID */
  def toOpaqueString(uuid: UUID): String =
    uuid.toString.replace("-", "")

  /** Returns an opaque string from the given UUID that is sequential w.r.t.
    * lexigraphical ordering for UUID Type-1 variants. That is, if a: UUID and
    * b: UUID and `b` was generated after `a` then
    *
    *   `toSequentialString(a) < toSequentialString(b) == true`
    *
    * returns None if the given UUID is not Type-1.
    *
    * See https://www.ietf.org/rfc/rfc4122.txt
    */
  def toSequentialString(uuid: UUID): Option[String] =
    if (uuid.version === 1) {
      val parts = uuid.toString.split("-")
      // ORIGINAL:   time.low-time.mid-ver.time.high-clockseq-node
      // SEQUENTIAL: ver.time.high-time.mid-time.low-clockseq-node
      // NB: The original hyphens are elided as the result no longer purports
      //     to be a UUID.
      Some(s"${parts(2)}${parts(1)}${parts(0)}${parts(3)}${parts(4)}")
    } else None
}

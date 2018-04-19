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

package quasar.qscript.provenance

import slamdata.Predef.{Char, Int}
import quasar.Qspec

import matryoshka.data.Fix
import scalaz.{IList, NonEmptyList}
import scalaz.std.anyVal._

object ProvSpec extends Qspec {

  val P = Prov[Char, Int, Fix[ProvF[Char, Int, ?]]](_.toInt)

  val p1 = P.proj('x')
  val p1i = 'x'.toInt

  val p2 = P.proj('y')
  val p2i = 'y'.toInt

  val v1i = 1
  val v1 = P.value(v1i)
  val v2i = 2
  val v2 = P.value(v2i)
  val v3i = 3
  val v3 = P.value(v3i)
  val v4i = 4
  val v4 = P.value(v4i)

  "provenance" should {
    "join keys" >> {
      "p1 θ p2" >> {
        P.joinKeys(p1, p2) must_= JoinKeys.empty
      }

      "value θ proj" >> {
        P.joinKeys(v1, p1) must_=
          JoinKeys.singleton(v1i, p1i)
      }

      "value θ value" >> {
        P.joinKeys(v1, v2) must_=
          JoinKeys.singleton(v1i, v2i)
      }

      "(v1 /\\\\ v2) θ v3" >> {
        P.joinKeys(P.both(v1, v2), v3) must_=
          JoinKeys(IList(NonEmptyList(
            JoinKey(v1i, v3i)
          , JoinKey(v2i, v3i))))
      }

      "(v1 /\\\\ v2) θ (v3 /\\\\ v4)" >> {
        P.joinKeys(P.both(v1, v2), P.both(v3, v4)) must_=
          JoinKeys(IList(NonEmptyList(
            JoinKey(v1i, v3i)
          , JoinKey(v1i, v4i)
          , JoinKey(v2i, v3i)
          , JoinKey(v2i, v4i))))
      }

      "(v1 /\\\\ v2) θ (v3 \\// v4)" >> {
        P.joinKeys(P.both(v1, v2), P.oneOf(v3, v4)) must_=
          JoinKeys(IList(NonEmptyList(
            JoinKey(v1i, v3i)
          , JoinKey(v1i, v4i)
          , JoinKey(v2i, v3i)
          , JoinKey(v2i, v4i))))
      }

      "(v1 /\\\\ v2) θ (v3 << v4)" >> {
        P.joinKeys(P.both(v1, v2), P.thenn(v3, v4)) must_=
          JoinKeys(IList(NonEmptyList(
            JoinKey(v1i, v4i)
          , JoinKey(v2i, v4i))))
      }

      "(v2 << v1) θ v3" >> {
        P.joinKeys(P.thenn(v2, v1), v3) must_=
          JoinKeys.singleton(v1i, v3i)
      }

      "(v2 << v1) θ (v4 << v3)" >> {
        P.joinKeys(P.thenn(v2, v1), P.thenn(v4, v3)) must_=
          JoinKeys(IList(
            NonEmptyList(JoinKey(v1i, v3i))
          , NonEmptyList(JoinKey(v2i, v4i))))
      }

      "(∅ \\// v2) θ v3" >> {
        P.joinKeys(P.oneOf(P.nada(), v2), v3) must_=
          JoinKeys.singleton(v2i, v3i)
      }

      "(∅ \\// v2) θ (v3 \\// ∅)" >> {
        P.joinKeys(P.oneOf(P.nada(), v2), P.oneOf(v3, P.nada())) must_=
          JoinKeys.singleton(v2i, v3i)
      }

      "(v1 \\// v2) θ (v3 \\// v4)" >> {
        P.joinKeys(P.oneOf(v1, v2), P.oneOf(v3, v4)) must_=
          JoinKeys(IList(NonEmptyList(
            JoinKey(v1i, v3i)
          , JoinKey(v1i, v4i)
          , JoinKey(v2i, v3i)
          , JoinKey(v2i, v4i))))
      }

      "(v1 \\// ∅) θ (v4 << v3)" >> {
        P.joinKeys(P.oneOf(v1, P.nada()), P.thenn(v4, v3)) must_=
          JoinKeys.singleton(v1i, v3i)
      }

      "((v1 /\\\\ v2) \\// v3) θ v4" >> {
        P.joinKeys(P.oneOf(P.both(v1, v2), v3), v4) must_=
          JoinKeys(IList(NonEmptyList(
            JoinKey(v1i, v4i)
          , JoinKey(v2i, v4i)
          , JoinKey(v3i, v4i))))
      }

      "((∅ \\// v2) /\\\\ v3) θ v4" >> {
        P.joinKeys(P.both(P.oneOf(P.nada(), v2), v3), v4) must_=
          JoinKeys(IList(NonEmptyList(
            JoinKey(v2i, v4i)
          , JoinKey(v3i, v4i))))
      }
    }
  }
}

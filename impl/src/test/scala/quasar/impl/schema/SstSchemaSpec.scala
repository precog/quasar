/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.impl.schema

import slamdata.Predef.None
import quasar.contrib.iota._
import quasar.contrib.matryoshka.envT
import quasar.ejson.{EJson, Fixed}
import quasar.ejson.implicits._
import quasar.sst._
import quasar.tpe._

import matryoshka.data.Fix
import matryoshka.implicits._
import scalaz._, Scalaz._
import shims._
import spire.math.Real

object SstSchemaSpec extends quasar.Qspec {
  import StructuralType.TypeST

  type J = Fix[EJson]
  type S = SST[J, Real]
  type T = StructuralType[J, Occurred[Real, TypeStat[Real]]]

  val J = Fixed[J]

  implicit val realShow: Show[Real] =
    Show.showFromToString

  "from sampled" >> {
    val sst = envT(
      TypeStat.coll(Real(10), Real(2).some, Real(4).some),
      TypeST(TypeF.arr[J, S](IList(
        envT(
          TypeStat.fromEJson(Real(10), J.str("one")),
          TypeST(TypeF.simple[J, S](SimpleType.Str))).embed,
        envT(
          TypeStat.fromEJson(Real(7), J.str("two")),
          TypeST(TypeF.simple[J, S](SimpleType.Str))).embed,
        envT(
          TypeStat.fromEJson(Real(4), J.str("three")),
          TypeST(TypeF.simple[J, S](SimpleType.Str))).embed,
        envT(
          TypeStat.fromEJson(Real(2), J.str("four")),
          TypeST(TypeF.simple[J, S](SimpleType.Str))).embed
      ), None))).embed

    "computes absolute occurrence" >> {
      val schema = SstSchema.fromSampled(Real(10), sst)

      val expected = SstSchema.sampleSchema(envT(
        Occurred(
          Real(1),
          TypeStat.coll(Real(10), Real(2).some, Real(4).some)),
        TypeST(TypeF.arr[J, T](IList(
          envT(
            Occurred(
              Real(1),
              TypeStat.fromEJson(Real(10), J.str("one"))),
            TypeST(TypeF.simple[J, T](SimpleType.Str))).embed,
          envT(
            Occurred(
              Real(7) / Real(10),
              TypeStat.fromEJson(Real(7), J.str("two"))),
            TypeST(TypeF.simple[J, T](SimpleType.Str))).embed,
          envT(
            Occurred(
              Real(4) / Real(10),
              TypeStat.fromEJson(Real(4), J.str("three"))),
            TypeST(TypeF.simple[J, T](SimpleType.Str))).embed,
          envT(
            Occurred(
              Real(2) / Real(10),
              TypeStat.fromEJson(Real(2), J.str("four"))),
            TypeST(TypeF.simple[J, T](SimpleType.Str))).embed
        ), None))).embed)

      schema must_= expected
    }

    "returns population when SST size < sample" >> {
      val schema = SstSchema.fromSampled(Real(100), sst)
      SstSchema.populationSchema.nonEmpty(schema) must beTrue
    }
  }
}

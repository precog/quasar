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

package quasar.fs

import slamdata.Predef._
import quasar.{Data, Fixture}
import quasar.sql._

import scalaz.Scalaz._

class ConstantPlansSpec extends quasar.Qspec {
  "asConstant should identify plans that are constant" >> {
    "addition" >> {
      val expr = sqlE"select 1 + 1 as maxlit from cars"
      constantPlans.asConstant(Fixture.unsafeToLP(expr)) must_=
        List(Data.Obj(ListMap("maxlit" -> Data.Int(2)))).some
    }
  }
}
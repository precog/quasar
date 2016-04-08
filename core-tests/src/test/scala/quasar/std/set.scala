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

package quasar.std

import quasar.Predef._
import quasar.TypeArbitrary
import quasar.fp._
import quasar.specs2.PendingWithAccurateCoverage

import org.specs2.ScalaCheck
import org.specs2.mutable._
import org.specs2.scalaz._

class SetSpec extends Specification with ScalaCheck with TypeArbitrary with ValidationMatchers with PendingWithAccurateCoverage {
  import SetLib._
  import quasar.Data
  import quasar.Type
  import quasar.Type.Const

  "SetLib" should {
    "type taking no results" in {
      val expr = Take(Type.Int, Type.Const(Data.Int(0)))
      expr should beSuccessful(Type.Const(Data.Set(Nil)))
    }

    "type filtering by false" in {
      val expr = Filter(Type.Int, Type.Const(Data.Bool(false)))
      expr should beSuccessful(Type.Const(Data.Set(Nil)))
    }

    "type inner join on false" in {
      val expr = InnerJoin(Type.Int, Type.Int, Type.Const(Data.Bool(false)))
      expr should beSuccessful(Type.Const(Data.Set(Nil)))
    }

    "type inner join with empty left" in {
      val expr = InnerJoin(Type.Const(Data.Set(Nil)), Type.Int, Type.Bool)
      expr should beSuccessful(Type.Const(Data.Set(Nil)))
    }

    "type inner join with empty right" in {
      val expr = InnerJoin(Type.Int, Type.Const(Data.Set(Nil)), Type.Bool)
      expr should beSuccessful(Type.Const(Data.Set(Nil)))
    }

    "type left outer join with empty left" in {
      val expr = LeftOuterJoin(Type.Const(Data.Set(Nil)), Type.Int, Type.Bool)
      expr should beSuccessful(Type.Const(Data.Set(Nil)))
    }

    "type right outer join with empty right" in {
      val expr = RightOuterJoin(Type.Int, Type.Const(Data.Set(Nil)), Type.Bool)
      expr should beSuccessful(Type.Const(Data.Set(Nil)))
    }

    "maintain first type for constantly" ! prop { (t1 : Type, t2 : Type) =>
      val expr = Constantly(t1, t2)
      (t1, t2) match {
        case (Const(r), Const(Data.Set(l))) =>
           expr must beSuccessful(Const(Data.Set(l.map(κ(r)))))
        case (_, _) => expr must beSuccessful(t1)
      }
    }
  }
}

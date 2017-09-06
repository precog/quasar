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

package quasar.std

import slamdata.Predef._
import quasar.{Func, TypeArbitrary}
import quasar.fp.ski._

class SetSpec extends quasar.Qspec with TypeArbitrary {
  import SetLib._
  import quasar.Data
  import quasar.Type
  import quasar.Type.Const

  "SetLib" should {
    "type taking no results" in {
      val expr = Take.tpe(Func.Input2(Type.Int, Type.Const(Data.Int(0))))
      expr should beSuccess(Type.Const(Data.Set(Nil)))
    }

    "type filtering by false" in {
      val expr = Filter.tpe(Func.Input2(Type.Int, Type.Const(Data.Bool(false))))
      expr should beSuccess(Type.Const(Data.Set(Nil)))
    }

    "type inner join on false" in {
      val expr = InnerJoin.tpe(Func.Input3(Type.Int, Type.Int, Type.Const(Data.Bool(false))))
      expr should beSuccess(Type.Const(Data.Set(Nil)))
    }

    "type inner join with empty left" in {
      val expr = InnerJoin.tpe(Func.Input3(Type.Const(Data.Set(Nil)), Type.Int, Type.Bool))
      expr should beSuccess(Type.Const(Data.Set(Nil)))
    }

    "type inner join with empty right" in {
      val expr = InnerJoin.tpe(Func.Input3(Type.Int, Type.Const(Data.Set(Nil)), Type.Bool))
      expr should beSuccess(Type.Const(Data.Set(Nil)))
    }

    "type left outer join with empty left" in {
      val expr = LeftOuterJoin.tpe(Func.Input3(Type.Const(Data.Set(Nil)), Type.Int, Type.Bool))
      expr should beSuccess(Type.Const(Data.Set(Nil)))
    }

    "type right outer join with empty right" in {
      val expr = RightOuterJoin.tpe(Func.Input3(Type.Int, Type.Const(Data.Set(Nil)), Type.Bool))
      expr should beSuccess(Type.Const(Data.Set(Nil)))
    }

    "maintain first type for constantly" >> prop { (t1 : Type, t2 : Type) =>
      val expr = Constantly.tpe(Func.Input2(t1, t2))
      (t1, t2) match {
        case (Const(r), Const(Data.Set(l))) =>
           expr must beSuccess(Const(Data.Set(l.map(κ(r)))))
        case (_, _) => expr must beSuccess(t1)
      }
    }.set(maxSize = 10)
  }
}

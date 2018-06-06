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

package quasar.ejson

import slamdata.Predef.{Map => SMap, _}
import quasar.contrib.iota.{:<<:, ACopK}

import jawn.{Facade, SimpleFacade, SupportParser}
import matryoshka._
import matryoshka.implicits._
import scalaz.Functor

object jsonParser {
  def apply[T, F[a] <: ACopK[a] : Functor]
    (implicit T: Corecursive.Aux[T, F], C: Common :<<: F, O: Obj :<<: F)
      : SupportParser[T] =
    new SupportParser[T] {
      implicit val facade: Facade[T] =
        new SimpleFacade[T] {
          def jarray(arr: List[T])          = C(Arr(arr)).embed
          def jobject(obj: SMap[String, T]) = O(Obj(ListMap(obj.toList: _*))).embed
          def jnull()                       = C(Null[T]()).embed
          def jfalse()                      = C(Bool[T](false)).embed
          def jtrue()                       = C(Bool[T](true)).embed
          def jnum(n: String)               = C(Dec[T](BigDecimal(n))).embed
          def jint(n: String)               = C(Dec[T](BigDecimal(n))).embed
          def jstring(s: String)            = C(Str[T](s)).embed
        }
    }
}

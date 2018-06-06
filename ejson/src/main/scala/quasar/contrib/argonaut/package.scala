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

package quasar.contrib

import quasar.ejson
import quasar.RenderTree
import quasar.contrib.iota.copkTraverse

import scala.collection.immutable.ListMap

import _root_.argonaut._, Argonaut._
import _root_.matryoshka._
import _root_.scalaz._
import iotaz.CopK

package object argonaut {
  private val CJ = CopK.Inject[ejson.Common, ejson.Json]
  private val OJ = CopK.Inject[ejson.Obj,    ejson.Json]

  implicit def jsonRecursive: Recursive.Aux[Json, ejson.Json] =
    new Recursive[Json] {
      type Base[A] = ejson.Json[A]

      def project(t: Json)(implicit BF: Functor[Base]) =
        t.fold(
          CJ(ejson.Null()),
          b => CJ(ejson.Bool(b)),
          d => CJ(ejson.Dec(d.toBigDecimal)),
          s => CJ(ejson.Str(s)),
          a => CJ(ejson.Arr(a)),
          o => OJ(ejson.Obj(ListMap(o.toList: _*))))
    }

  implicit def jsonCorecursive: Corecursive.Aux[Json, ejson.Json] =
    new Corecursive[Json] {
      type Base[A] = ejson.Json[A]

      def embed(ft: ejson.Json[Json])(implicit BF: Functor[Base]) =
        ft match {
          case CJ(ejson.Null())  => jNull
          case CJ(ejson.Bool(b)) => jBool(b)
          case CJ(ejson.Dec(d))  => jNumber(d)
          case CJ(ejson.Str(s))  => jString(s)
          case CJ(ejson.Arr(a))  => jArray(a)
          case OJ(ejson.Obj(o))  => jObject(JsonObject.fromTraversableOnce(o))
        }
    }

  implicit def jsonRenderTree: RenderTree[Json] = RenderTree.recursive[Json, ejson.Json]
}

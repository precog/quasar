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

package quasar.physical.mongodb.planner

import slamdata.Predef.{Map => _, _}
import quasar.fp._
import quasar.qscript._
import quasar.qscript.construction
import quasar.qscript.RecFreeS._

import matryoshka.{Hole => _, _}
import matryoshka.implicits._
import scalaz._, Scalaz._

object mapBeforeSort {

  def apply[T[_[_]]: BirecursiveT, M[_]: Applicative]: Trans[QScriptCore[T, ?], M] =
    new Trans[QScriptCore[T, ?], M] {

      private val func = construction.Func[T]
      private val recFunc = construction.RecFunc[T]
      private def projectIndex(i: Int): FreeMap[T] = func.ProjectIndexI(func.Hole, i)

      def trans[A, G[_]: Functor]
        (GtoF: PrismNT[G, QScriptCore[T, ?]])
        (implicit TC: Corecursive.Aux[A, G], TR: Recursive.Aux[A, G])
          : QScriptCore[T, A] => M[G[A]] = qs => doTrans[A, G](GtoF).apply(qs).point[M]

      private def doTrans[A, G[_]: Functor]
        (GtoF: PrismNT[G, QScriptCore[T, ?]])
        (implicit TC: Corecursive.Aux[A, G], TR: Recursive.Aux[A, G])
          : QScriptCore[T, A] => G[A] = {
        case qs @ Map(src, fm) =>
          GtoF.get(src.project) match {
            case Some(Sort(innerSrc, bucket, order)) =>
              val innerMap =
                GtoF.reverseGet(Map(
                  innerSrc,
                  MapFuncCore.RecStaticArray(List(fm, recFunc.Hole)))).embed
              val m = Map(
                GtoF.reverseGet(Sort(innerMap,
                  bucket.map(_ >> projectIndex(1)),
                  order.map {
                    case (fm, dir) =>
                      (fm >> projectIndex(1), dir)
                  })).embed,
                projectIndex(0).asRec)
              GtoF.reverseGet(m)
            case _ => GtoF.reverseGet(qs)
          }
        case x => GtoF.reverseGet(x)
      }
  }
}

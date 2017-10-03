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

package quasar.physical.mongodb.expression.transform

import slamdata.Predef._

import quasar.physical.mongodb.expression._

import matryoshka._
import matryoshka.implicits._
import scalaz._

object wrapArrayInLet {
  def apply[T[_[_]]: CorecursiveT, EX[_]: Functor]
    (expr: EX[T[EX]])
    (implicit ev: ExprOpCoreF :<: EX, ev32: ExprOp3_2F :<: EX)
      : EX[T[EX]] = expr match {
    case a @ $arrayLitF(_) =>
      $letF(ListMap(DocVar.Name("a") -> a.embed),
        $varF[EX, T[EX]](DocVar.Name("a")()).embed)
    case x => x
  }
}

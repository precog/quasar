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

package quasar.physical.mongodb.planner

import slamdata.Predef._
import quasar.physical.mongodb.expression._
import quasar.qscript._

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import scalaz._, Scalaz._

/**
 * Translates static QScript structures, which cannot currently be handled
 * by `FuncHandler`, because they are not basic elements of QScript.
 *
 * TODO: once QScript supports multiple arguments for `MakeMap` and `MakeArray`,
 * we should be able to remove this.
 */
trait StaticHandler[T[_[_]], EX[_]] {
  def handle[A](fm: FreeMapA[T, A]): Option[EX[FreeMapA[T, A]]]
}

object StaticHandler {

  def v2_6[T[_[_]]: BirecursiveT, EX[_]: Traverse, A]
    (implicit e26: ExprOpCoreF :<: EX)
      : StaticHandler[T, EX] =
    new StaticHandler[T, EX] {
      def handle[A](fm: FreeMapA[T, A]): Option[EX[FreeMapA[T, A]]] =
        None
    }

  def v3_2[T[_[_]]: BirecursiveT, EX[_]: Traverse, A]
    (implicit e26: ExprOpCoreF :<: EX, e30: ExprOp3_0F :<: EX, e32: ExprOp3_2F :<: EX)
      : StaticHandler[T, EX] =
    new StaticHandler[T, EX] {
      def handle[A](fm: FreeMapA[T, A]): Option[EX[FreeMapA[T, A]]] = {
        val h3_2 = fm.project.some collect {
          case MapFuncCore.StaticArray(a) =>
            $arrayLitF(a)
        }
        h3_2 orElse v2_6[T, EX, A].handle(fm)
      }
    }
}

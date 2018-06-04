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

import slamdata.Predef._

import quasar.ejson.{EJson, Fixed}
import quasar.physical.mongodb.BsonField
import quasar.physical.mongodb.expression._
import quasar.qscript._
import quasar.contrib.iota.copkTraverse

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

  def handle[T[_[_]]: BirecursiveT, EX[_]: Traverse, A]
    (implicit ev: ExprOpCoreF :<: EX)
      : StaticHandler[T, EX] =
    new StaticHandler[T, EX] {
      def toBsonFieldName(ej: T[EJson]): Option[BsonField.Name] =
        Fixed[T[EJson]].str.getOption(ej) map (BsonField.Name(_))

      def handle[A](fm: FreeMapA[T, A]): Option[EX[FreeMapA[T, A]]] =
        fm.project match {
          case MapFuncCore.StaticMap(m) =>
            val x: Option[List[(BsonField.Name, FreeMapA[T, A])]] =
              m.traverse(t => toBsonFieldName(t._1) map ((_, t._2)))
            x.map(l => $objectLitF(ListMap(l : _*)))
          case MapFuncCore.StaticArray(a) =>
            $arrayLitF(a).some
          case _ => none
        }
    }
}

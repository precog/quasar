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

package quasar

import quasar.Predef.Boolean

import java.lang.String
import scala.Predef.implicitly

import matryoshka._
import matryoshka.implicits._
import monocle.Prism
import scalaz._

package object ejson {
  def str[A] = Prism.partial[Common[A], String] { case Str(s) => s } (Str(_))

  /** For _strict_ JSON, you want something like `Obj[Mu[Json]]`.
    */
  type Json[A] = Coproduct[Obj, Common, A]

  type EJson[A] = Coproduct[Extension, Common, A]

  val ExtEJson = implicitly[Extension :<: EJson]
  val CommonEJson = implicitly[Common :<: EJson]

  object EJson {
    def fromJson[A](f: String => A): Json[A] => EJson[A] =
      json => Coproduct(json.run.leftMap(Extension.fromObj(f)))

    def fromJsonT[T[_[_]]: BirecursiveT]: T[Json] => T[EJson] =
      _.transAna[T[EJson]](fromJson(s => Coproduct.right[Obj](str[T[Json]](s)).embed))

    def fromCommon[T](implicit T: Corecursive.Aux[T, EJson]): Common[T] => T =
      CommonEJson.inj(_).embed

    def fromExt[T](implicit T: Corecursive.Aux[T, EJson]): Extension[T] => T =
      ExtEJson.inj(_).embed

    def isNull[T](ej: T)(implicit T: Recursive.Aux[T, EJson]): Boolean =
      CommonEJson.prj(ej.project).fold(false) {
        case ejson.Null() => true
        case _ => false
    }
  }
}

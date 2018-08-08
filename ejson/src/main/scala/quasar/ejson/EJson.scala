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

import slamdata.Predef.{Char => SChar, Int => _, Map => _, _}
import quasar.contrib.matryoshka.{project, totally}
import quasar.contrib.iota.copkTraverse

import matryoshka._
import matryoshka.implicits._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.syntax.traverse._

object EJson {
  def fromJson[A](f: String => A): Json[A] => EJson[A] = {
    case ObjJson(obj) => ExtEJson(Extension.fromObj(f)(obj))
    case CommonJson(c) => CommonEJson(c)
  }

  def fromCommon[T](c: Common[T])(implicit T: Corecursive.Aux[T, EJson]): T =
    CommonEJson(c).embed

  def fromExt[T](e: Extension[T])(implicit T: Corecursive.Aux[T, EJson]): T =
    ExtEJson(e).embed

  def toJson[A](f: A => Option[String]): EJson[A] => Option[Json[A]] = {
    val handleExt: Extension[A] => Option[Json[A]] = {
      case Map(xs) =>
        xs.traverse {
          case (k, v) => f(k) strengthR v
        } map (kvs => ObjJson(Obj(ListMap(kvs : _*))))

      case Int(i) =>
        some(CommonJson(Dec(BigDecimal(i))))

      case _ => none
    }

    {
      case ExtEJson(ext) => handleExt(ext)
      case CommonEJson(c) => some(CommonJson(c))
    }
  }

  def arr[T](xs: T*)(implicit T: Corecursive.Aux[T, EJson]): T =
    fromCommon(Arr(xs.toList))

  def bool[T](b: Boolean)(implicit T: Corecursive.Aux[T, EJson]): T =
    fromCommon(Bool(b))

  def char[T](c: SChar)(implicit T: Corecursive.Aux[T, EJson]): T =
    fromExt(Char(c))

  def dec[T](d: BigDecimal)(implicit T: Corecursive.Aux[T, EJson]): T =
    fromCommon(Dec(d))

  def int[T](d: BigInt)(implicit T: Corecursive.Aux[T, EJson]): T =
    fromExt(Int(d))

  def map[T](xs: (T, T)*)(implicit T: Corecursive.Aux[T, EJson]): T =
    fromExt(Map(xs.toList))

  def meta[T](v: T, m: T)(implicit T: Corecursive.Aux[T, EJson]): T =
    fromExt(Meta(v, m))

  def nul[T](implicit T: Corecursive.Aux[T, EJson]): T =
    fromCommon(Null())

  def obj[T](xs: (String, T)*)(implicit T: Corecursive.Aux[T, EJson]): T =
    map((xs.map { case (s, t) => str[T](s) -> t }): _*)

  def str[T](s: String)(implicit T: Corecursive.Aux[T, EJson]): T =
    fromCommon(Str(s))

  def isNull[T](ej: T)(implicit T: Recursive.Aux[T, EJson]): Boolean =
    project[T, EJson].composePrism(optics.nul).nonEmpty(ej)

  /** Replaces `Meta` nodes with their value component. */
  def elideMetadata[T](
    implicit T: Recursive.Aux[T, EJson]
  ): EJson[T] => EJson[T] = totally {
    case ExtEJson(Meta(v, _)) => v.project
  }
}

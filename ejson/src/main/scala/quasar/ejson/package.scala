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

package quasar

import slamdata.Predef._
import quasar.contrib.matryoshka._
import quasar.fp.ski._

import java.lang.String
import scala.Predef.implicitly
import scala.Unit
import scala.collection.immutable.{List, ListMap}
import scala.math.{BigDecimal, BigInt}

import matryoshka._
import matryoshka.implicits._
import monocle.{Iso, Prism}
import scalaz._, Scalaz._

package object ejson {
  def arr[A] =
    Prism.partial[Common[A], List[A]] { case Arr(a) => a } (Arr(_))

  def bool[A] =
    Prism.partial[Common[A], Boolean] { case Bool(b) => b } (Bool(_))

  def dec[A] =
    Prism.partial[Common[A], BigDecimal] { case Dec(bd) => bd } (Dec(_))

  def nul[A] =
    Prism.partial[Common[A], Unit] { case Null() => () } (κ(Null()))

  def str[A] =
    Prism.partial[Common[A], String] { case Str(s) => s } (Str(_))

  def obj[A] =
    Iso[Obj[A], ListMap[String, A]](_.value)(Obj(_))

  def byte[A] =
    Prism.partial[Extension[A], scala.Byte] { case Byte(b) => b } (Byte(_))

  def char[A] =
    Prism.partial[Extension[A], scala.Char] { case Char(c) => c } (Char(_))

  def int[A] =
    Prism.partial[Extension[A], BigInt] { case Int(i) => i } (Int(_))

  def map[A] =
    Prism.partial[Extension[A], List[(A, A)]] { case Map(m) => m } (Map(_))

  def meta[A] =
    Prism.partial[Extension[A], (A, A)] {
      case Meta(v, m) => (v, m)
    } ((Meta(_: A, _: A)).tupled)

  /** For _strict_ JSON, you want something like `Obj[Mu[Json]]`.
    */
  type Json[A]    = Coproduct[Obj, Common, A]
  val ObjJson     = implicitly[Obj :<: Json]
  val CommonJson  = implicitly[Common :<: Json]

  type EJson[A]   = Coproduct[Extension, Common, A]
  val ExtEJson    = implicitly[Extension :<: EJson]
  val CommonEJson = implicitly[Common :<: EJson]

  object EJson {
    def fromJson[A](f: String => A): Json[A] => EJson[A] =
      json => Coproduct(json.run.leftMap(Extension.fromObj(f)))

    def fromCommon[T](c: Common[T])(implicit T: Corecursive.Aux[T, EJson]): T =
      CommonEJson(c).embed

    def fromExt[T](e: Extension[T])(implicit T: Corecursive.Aux[T, EJson]): T =
      ExtEJson(e).embed

    def isNull[T](ej: T)(implicit T: Recursive.Aux[T, EJson]): Boolean =
      CommonEJson.prj(ej.project) exists (nul.nonEmpty(_))

    /** Replaces `Meta` nodes with their value component. */
    def elideMetadata[T](
      implicit T: Recursive.Aux[T, EJson]
    ): EJson[T] => EJson[T] = totally {
      case ExtEJson(Meta(v, _)) => v.project
    }

    /** Replace a string with an array of characters. */
    def replaceString[T](
      implicit T: Corecursive.Aux[T, EJson]
    ): EJson[T] => EJson[T] = totally {
      case CommonEJson(Str(s)) => CommonEJson(arr[T](s.toList map (c => fromExt(char[T](c)))))
    }

    /** Replace an array of characters with a string. */
    def restoreString[T](
      implicit
      TC: Corecursive.Aux[T, EJson],
      TR: Recursive.Aux[T, EJson]
    ): EJson[T] => EJson[T] = totally {
      case a @ CommonEJson(Arr(t :: ts)) =>
        (t :: ts).traverse(t => ExtEJson.prj(t.project) >>= (char[T].getOption(_)))
          .fold(a)(cs => CommonEJson(str[T](cs.mkString)))
    }
  }


  final case class TypeTag(value: String) extends scala.AnyVal

  object TypeTag {
    val Binary    = TypeTag("_ejson.binary")
    val Date      = TypeTag("_ejson.date")
    val Interval  = TypeTag("_ejson.interval")
    val Time      = TypeTag("_ejson.time")
    val Timestamp = TypeTag("_ejson.timestamp")

    val stringIso: Iso[TypeTag, String] =
      Iso[TypeTag, String](_.value)(TypeTag(_))

    implicit val encodeEJson: EncodeEJson[TypeTag] =
      EncodeEJson[String].contramap(_.value)

    implicit val order: Order[TypeTag] =
      Order.orderBy(_.value)

    implicit val show: Show[TypeTag] =
      Show.showFromToString
  }

  object Type {
    import EJson._

    val TypeKey = "_ejson.type"

    def apply[T](tag: TypeTag)(implicit T: Corecursive.Aux[T, EJson]): T =
      fromExt(Map(List(fromCommon[T](Str(TypeKey)) -> fromCommon[T](Str(tag.value)))))

    /** Extracts the type tag from a metadata map, if present. */
    def unapply[T](ejs: EJson[T])(implicit T: Recursive.Aux[T, EJson]): Option[TypeTag] =
      ejs match {
        case ExtEJson(Map(xs)) =>
          xs collectFirst {
            case (Embed(CommonEJson(Str(TypeKey))), Embed(CommonEJson(Str(t)))) => TypeTag(t)
          }
        case _ => none
      }
  }

  object SizedType {
    import EJson._

    val SizeKey = "_ejson.size"

    def apply[T](tag: TypeTag, size: BigInt)(implicit T: Corecursive.Aux[T, EJson]): T =
      fromExt(Map(List(
        fromCommon[T](Str(Type.TypeKey)) -> fromCommon[T](Str(tag.value)),
        fromCommon[T](Str(SizeKey))      -> fromExt[T](Int(size))
      )))

    /** Extracts the type tag and size from a metadata map, if both are present. */
    def unapply[T](ejs: EJson[T])(implicit T: Recursive.Aux[T, EJson]): Option[(TypeTag, BigInt)] =
      ejs match {
        case ExtEJson(Map(xs)) =>
          val size = xs collectFirst {
            case (Embed(CommonEJson(Str(SizeKey))), Embed(ExtEJson(Int(s)))) => s
          }
          Type.unapply(ejs) tuple size

        case _ => none
      }
  }
}

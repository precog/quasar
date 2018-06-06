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

import slamdata.Predef.{Char => SChar, _}
import quasar.contrib.matryoshka._
import quasar.fp.ski.κ
import quasar.contrib.iota.copkTraverse

import matryoshka._
import matryoshka.implicits._
import matryoshka.patterns.CoEnv
import monocle.Prism
import scalaz._, Scalaz.{char => charz, _}

object JsonCodec {
  import Common.{Optics => C}

  /** Encode an EJson value as Json.
    *
    * In order to remain compatible with JSON and achieve a compact encoding
    * the following scheme is used:
    *
    * 1. All JSON compatible values are encoded verbatim.
    *
    * 2. Extended values are encoded as
    *
    *    Meta -> { ∃value: ..., ∃meta: ...}
    *    Map  -> { ∃map: [{∃key: ..., ∃value: ...}, ...] }
    *    Byte -> { ∃byte: 42 }
    *    Char -> { ∃char: "x" }
    *    Int  -> { ∃int: 2345 }
    *
    * 3. A Map where the keys are all strings will be encoded as a Json object
    *    with encoded EJson values.
    *
    * 4. Any map keys in a source EJson value that begin with '∃' are prefixed with an
    *    additional '∃'.
    */
  def encodeƒ[J](
    implicit
    JC: Corecursive.Aux[J, Json],
    JR: Recursive.Aux[J, Json]
  ): Algebra[EJson, J] = {
    case CommonEJson(c) =>
      CommonJson(c).embed

    case ExtEJson(Meta(v, m)) =>
      MetaObj(v, m).embed

    case ExtEJson(Byte(b)) =>
      SingletonObj(ByteK, CommonJson(C.dec[J](BigDecimal(b.toInt))).embed).embed

    case ExtEJson(Char(c)) =>
      SingletonObj(CharK, OneChar[J](c).embed).embed

    case ExtEJson(Int(i)) =>
      SingletonObj(IntK, CommonJson(C.dec[J](BigDecimal(i))).embed).embed

    case ExtEJson(Map(entries)) =>
      val sigildEntries =
        entries.map(_.leftMap(_.mapR(sigildJs)))

      val maybeObj =
        sigildEntries.traverse({
          case (Embed(CommonJson(Str(k))), v) => some(k -> v)
          case _                              => none
        }) map (kvs => ObjJson(Obj(ListMap(kvs: _*))).embed)

      maybeObj getOrElse SingletonObj(MapK, MapArr(sigildEntries).embed).embed
  }

  /** Error describing why decoding the given value failed. */
  final case class DecodingFailed[A](reason: String, value: A)

  /** Attempt to decode an EJson value from Json. */
  def decodeƒ[J](
    implicit
    JC: Corecursive.Aux[J, Json],
    JR: Recursive.Aux[J, Json]
  ): Coalgebra[CoEnv[DecodingFailed[J], EJson, ?], J] =
    j => CoEnv(j.project match {
      case MetaObj(v, m) =>
        ExtEJson(Meta(v, m)).right

      case SingletonObj(`ByteK`, v) =>
        extractC(C.dec[J], v.project)
          .filter(_.isValidByte)
          .map(d => optics.byte[J](d.toByte))
          .toRightDisjunction(DecodingFailed("expected a byte", v))

      case SingletonObj(`CharK`, v) =>
        some(v.project)
          .collect { case OneChar(c) => optics.char[J](c) }
          .toRightDisjunction(DecodingFailed("expected a single character", v))

      case SingletonObj(`IntK`, v) =>
        extractC(C.dec[J], v.project)
          .flatMap(_.toBigIntExact.map(optics.int[J](_)))
          .toRightDisjunction(DecodingFailed("expected an integer", v))

      case SingletonObj(`MapK`, v) =>
        MapArr.unapply(v.project)
          .map(optics.map[J](_))
          .toRightDisjunction(DecodingFailed("expected an array of map entries", v))

      case ObjJson(Obj(m)) =>
        ExtEJson(Map(m.toList.map(_.leftMap(s =>
          CJ[J].composePrism(C.str)(unsigild(s)).embed
        )))).right

      case CommonJson(c) =>
        CommonEJson(c).right
    })

  /** Constants used in the Json encoding. */
  val Sigil   = '∃'
  val ByteK   = sigild("byte")
  val CharK   = sigild("char")
  val IntK    = sigild("int")
  val KeyK    = sigild("key")
  val MetaK   = sigild("meta")
  val MapK    = sigild("map")
  val ValueK  = sigild("value")
  val ExtKeys = ISet.fromList(List(ByteK, CharK, IntK, KeyK, MetaK, MapK, ValueK))

  ////

  private def CJ[A] = Prism[Json[A], Common[A]](CommonJson.prj.apply)(CommonJson.inj.apply)

  private def extractC[A, B](p: Prism[Common[A], B], j: Json[A]): Option[B] =
    CJ[A].composePrism(p).getOption(j)

  private def isSigild(s: String): Boolean =
    s.headOption.exists(_ ≟ Sigil)

  private def sigild(s: String): String =
    Sigil.toString + s

  private def unsigild(s: String): String =
    (isSigild(s) && ExtKeys.notMember(s)) ? s.drop(1) | s

  private def sigildJs[A]: Json[A] => Json[A] = totally {
    case CommonJson(Str(s)) if isSigild(s) => CommonJson(C.str[A](sigild(s)))
  }

  private object OneChar {
    def apply[A](c: SChar): Json[A] =
      CommonJson(Str(c.toString))

    def unapply[A](js: Json[A]): Option[SChar] =
      CJ[A].composePrism(C.str)
        .getOption(js)
        .flatMap(s => s.headOption.filter(κ(s.length ≟ 1)))
  }

  private object SingletonObj {
    def apply[A](k: String, v: A): Json[A] =
      ObjJson(Obj(ListMap(k -> v)))

    def unapply[A](js: Json[A]): Option[(String, A)] =
      ObjJson.prj(js) flatMap {
        case Obj(m) => m.headOption.filter(κ(m.size ≟ 1))
      }
  }

  private object MetaObj {
    def apply[A](v: A, m: A): Json[A] =
      ObjJson(Obj(ListMap(ValueK -> v, MetaK -> m)))

    def unapply[A](js: Json[A]): Option[(A, A)] =
      ObjJson.prj(js) flatMap {
        case Obj(m) => (m.get(ValueK) tuple m.get(MetaK)).filter(κ(m.size ≟ 2))
      }
  }

  private object MapArr {
    def apply[J](xs: List[(J, J)])(
      implicit J: Corecursive.Aux[J, Json]
    ): Json[J] =
      CommonJson(Arr(xs map { case (k, v) => MapEntry(k, v).embed }))

    def unapply[J](js: Json[J])(
      implicit J: Recursive.Aux[J, Json]
    ): Option[List[(J, J)]] =
      CJ[J].composePrism(C.arr)
        .getOption(js)
        .flatMap(_.traverse(j => MapEntry.unapply(j.project)))
  }

  private object MapEntry {
    def apply[A](k: A, v: A): Json[A] =
      ObjJson(Obj(ListMap(KeyK -> k, ValueK -> v)))

    def unapply[A](js: Json[A]): Option[(A, A)] =
      ObjJson.prj(js) flatMap {
        case Obj(m) => (m.get(KeyK) tuple m.get(ValueK)).filter(κ(m.size ≟ 2))
      }
  }
}

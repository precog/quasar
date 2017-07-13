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

package quasar.ejson

import slamdata.Predef._

import monocle.Prism
import scalaz._, Scalaz._
import scodec.bits.ByteVector

package object z85 {

  val mapping =
    ("0123456789" ++
      "abcdefghij" ++
      "klmnopqrst" ++
      "uvwxyzABCD" ++
      "EFGHIJKLMN" ++
      "OPQRSTUVWX" ++
      "YZ.-:+=^!/" ++
      "*?&<>()[]{" ++
      "}@%$#").toCharArray

  /** This takes a ByteArray containing exactly four bytes and returns a String
    * containing exactly five ASCII characters.
    */
  def encodeBlock(bytes: ByteVector): String = {
    val num1 = bytes.toLong(false)
    val index1 = num1 % 85
    val char1 = mapping(index1.toInt)
    val num2 = (num1 - index1) / 85
    val index2 = num2 % 85
    val char2 = mapping(index2.toInt)
    val num3 = (num2 - index2) / 85
    val index3 = num3 % 85
    val char3 = mapping(index3.toInt)
    val num4 = (num3 - index3) / 85
    val index4 = num4 % 85
    val char4 = mapping(index4.toInt)
    val num5 = (num4 - index4) / 85
    val index5 = num5 % 85
    val char5 = mapping(index5.toInt)

    java.lang.String.valueOf(Array(char5, char4, char3, char2, char1))
  }

  // TODO: this should really fail if unsupported chars are included
  val decodeBlock: String => Option[ByteVector] =
    str => ByteVector.fromLong(str.toList.map(mapping.indexOf(_).toLong).fzipWith(List(85 * 85 * 85 * 85, 85 * 85 * 85, 85 * 85, 85, 1))(_ * _).foldRight(0L)(_ + _), 4).some

  val encode: ByteVector => String =
    StreamT
      .unfold(_)(b =>
        if (b.size ≟ 0) None
        else (b.take(4).padTo(4), b.drop(4)).some)
      .foldLeftRec(Cord(""))((acc, b) => acc ++ Cord(encodeBlock(b))).toString

  val decode: String => Option[ByteVector] =
    StreamT
      .unfold(_)(s =>
        if (s.length ≟ 0) None
        else (s.take(5).padTo(5, '0').toString, s.drop(5).toString).some)
      .foldLeftM(ByteVector.empty)((acc, s) => decodeBlock(s).map(acc ++ _))

  val block = Prism[String, ByteVector](decodeBlock)(encodeBlock)
}

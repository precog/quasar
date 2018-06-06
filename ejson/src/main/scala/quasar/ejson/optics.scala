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

import slamdata.Predef.{Byte => SByte, Char => SChar, Int => _, Map => _, _}
import quasar.fp.PrismNT

import monocle.Prism
import scalaz.{==>>, Order}

object optics {
  import Common.{Optics => CO}
  import Extension.{Optics => EO}

  def com[A]: Prism[EJson[A], Common[A]] =
    PrismNT.injectCopK[Common, EJson].asPrism[A]

  def ext[A]: Prism[EJson[A], Extension[A]] =
    PrismNT.injectCopK[Extension, EJson].asPrism[A]

  def arr[A]: Prism[EJson[A], List[A]] =
    com composePrism CO.arr

  def bool[A]: Prism[EJson[A], Boolean] =
    com composePrism CO.bool

  def byte[A]: Prism[EJson[A], SByte] =
    ext composePrism EO.byte

  def char[A]: Prism[EJson[A], SChar] =
    ext composePrism EO.char

  def dec[A]: Prism[EJson[A], BigDecimal] =
    com composePrism CO.dec

  def int[A]: Prism[EJson[A], BigInt] =
    ext composePrism EO.int

  def imap[A: Order]: Prism[EJson[A], A ==>> A] =
    ext composePrism EO.imap

  def map[A]: Prism[EJson[A], List[(A, A)]] =
    ext composePrism EO.map

  def meta[A]: Prism[EJson[A], (A, A)] =
    ext composePrism EO.meta

  def nul[A]: Prism[EJson[A], Unit] =
    com composePrism CO.nul

  def str[A]: Prism[EJson[A], String] =
    com composePrism CO.str
}

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

package quasar.yggdrasil.table

import cats.effect.Sync

import tectonic.{Plate, Signal}

import scala.{Boolean, Int, Predef, StringContext, Unit}, Predef.{print, println}

import java.lang.CharSequence

// for debugging use only! (duh)
private[table] final class PrintlnPlate[A] private (
    delegate: Plate[A])
    extends Plate[A] {

  def nul(): Signal = {
    print("nul() -> ")
    val back = delegate.nul()
    println(back)
    back
  }

  def fls(): Signal = {
    print("fls() -> ")
    val back = delegate.fls()
    println(back)
    back
  }

  def tru(): Signal = {
    print("tru() -> ")
    val back = delegate.tru()
    println(back)
    back
  }

  def map(): Signal = {
    print("map() -> ")
    val back = delegate.map()
    println(back)
    back
  }

  def arr(): Signal = {
    print("arr() -> ")
    val back = delegate.arr()
    println(back)
    back
  }

  def num(s: CharSequence, decIdx: Int, expIdx: Int): Signal = {
    print(s"num($s, $decIdx, $expIdx) -> ")
    val back = delegate.num(s, decIdx, expIdx)
    println(back)
    back
  }

  def str(s: CharSequence): Signal = {
    print(s"str($s) -> ")
    val back = delegate.str(s)
    println(back)
    back
  }

  def nestMap(pathComponent: CharSequence): Signal = {
    print(s"nestMap($pathComponent) -> ")
    val back = delegate.nestMap(pathComponent)
    println(back)
    back
  }

  def nestArr(): Signal = {
    print("nestArr() -> ")
    val back = delegate.nestArr()
    println(back)
    back
  }

  def nestMeta(pathComponent: CharSequence): Signal = {
    print(s"nestMeta($pathComponent) -> ")
    val back = delegate.nestMeta(pathComponent)
    println(back)
    back
  }

  def unnest(): Signal = {
    print("unnest() -> ")
    val back = delegate.unnest()
    println(back)
    back
  }

  def finishRow(): Unit = {
    println("finishRow()")
    delegate.finishRow()
  }

  def finishBatch(terminal: Boolean): A = {
    println(s"finishBatch($terminal)")
    delegate.finishBatch(terminal)
  }

  override def skipped(bytes: Int): Unit = {
    println(s"skipped($bytes)")
    delegate.skipped(bytes)
  }
}

object PrintlnPlate {
  def apply[F[_]: Sync, A](delegate: Plate[A]): F[Plate[A]] =
    Sync[F].delay(new PrintlnPlate(delegate))
}

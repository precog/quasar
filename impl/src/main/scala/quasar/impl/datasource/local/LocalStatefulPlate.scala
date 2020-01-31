/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.impl.datasource.local

import slamdata.Predef._

import java.lang.CharSequence

import tectonic.{Plate, Signal}

final class LocalStatefulPlate() extends Plate[Unit] {

  private var sawSomething: Boolean = false
  private var state: Long = 0

  def updateState(inc: Long): Option[Long] = {
    state += inc
    val back = if (sawSomething) Some(state) else None
    sawSomething = false
    back
    }

  def arr(): Signal = update
  def map(): Signal = update
  def nul(): Signal = update
  def fls(): Signal = update
  def tru(): Signal = update
  def str(s: CharSequence): Signal = update
  def num(s: CharSequence, decIdx: Int, expIdx: Int): Signal = update

  def nestArr(): Signal = update
  def nestMap(pathComponent: CharSequence): Signal = update
  def nestMeta(pathComponent: CharSequence): Signal = update
  def unnest(): Signal = update

  def finishRow(): Unit = sawSomething = true
  def skipped(bytes: Int): Unit = sawSomething = true
  def finishBatch(terminal: Boolean): Unit = sawSomething = true

  ////

  private def update: Signal = {
    sawSomething = true
    Signal.Continue
  }
}

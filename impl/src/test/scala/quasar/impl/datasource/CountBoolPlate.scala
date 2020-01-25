/*
 * Copyright 2014–2019 SlamData Inc.
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

package quasar.impl.datasources

import slamdata.Predef._

import java.lang.CharSequence

import tectonic.{Plate, Signal}

final class CountBoolPlate() extends Plate[Unit] {

  private var state: Int = 0

  def getState(): Int = state

  def arr(): Signal = Signal.Continue
  def map(): Signal = Signal.Continue
  def nul(): Signal = Signal.Continue
  def str(s: CharSequence): Signal = Signal.Continue
  def num(s: CharSequence, decIdx: Int, expIdx: Int): Signal = Signal.Continue

  def fls(): Signal = {
    state += 1
    Signal.Continue
  }

  def tru(): Signal = {
    state += 1
    Signal.Continue
  }

  def nestArr(): Signal = Signal.Continue
  def nestMap(pathComponent: CharSequence): Signal = Signal.Continue
  def nestMeta(pathComponent: CharSequence): Signal = Signal.Continue
  def unnest(): Signal = Signal.Continue

  def finishRow(): Unit = ()
  def skipped(bytes: Int): Unit = ()
  def finishBatch(terminal: Boolean): Unit = ()
}

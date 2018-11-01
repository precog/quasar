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

import quasar.{CompositeParseType, IdStatus, ParseInstruction, ParseType}
import quasar.common.{CPath, CPathField, CPathIndex, CPathMeta, CPathNode}

import tectonic.{DelegatingPlate, Plate, Signal}

import scala.annotation.tailrec

// currently assumes retain = false, meaning you *cannot* have any non-shifted stuff in the row
private[table] final class PivotPlate[A](
    path: CPath,
    idStatus: IdStatus,
    structure: CompositeParseType,
    delegate: Plate[A])
    extends DelegatingPlate(delegate)
    with CPathPlate[A] {

  private val rfocus = path.nodes.reverse
  private val rfocusPlus1 = CPathIndex(1) :: rfocus

  // if this is true, it means we're under the focus and might unnest once without moving
  private var underFocus = false
  private var suppress = false

  // we pivoted at least once
  private var pivoted = false
  private var pivotedTwice = false

  private var scalarSkip = false

  // we can't trust nextIndex.head because it only considers post-shift data
  private var focusedArrayIndex = 0

  private val IdOnly = IdStatus.IdOnly
  private val IncludeId = IdStatus.IncludeId
  // private val ExcludeId = IdStatus.ExcludeId

  private val ObjectStruct = ParseType.Object
  private val ArrayStruct = ParseType.Array

  override def nul(): Signal = {
    if (suppress)
      Signal.Continue
    else
      super.nul()
  }

  override def fls(): Signal = {
    if (suppress)
      Signal.Continue
    else
      super.fls()
  }

  override def tru(): Signal = {
    if (suppress)
      Signal.Continue
    else
      super.tru()
  }

  override def map(): Signal = {
    if (suppress) {
      Signal.Continue
    } else if (atFocus() && (structure eq ObjectStruct)) {
      pivoted = true
      scalarSkip = true
      Signal.Continue
    } else {
      super.map()
    }
  }

  override def arr(): Signal = {
    if (suppress) {
      Signal.Continue
    } else if (atFocus() && (structure eq ArrayStruct)) {
      pivoted = true
      scalarSkip = true
      Signal.Continue
    } else {
      super.arr()
    }
  }

  override def num(s: CharSequence, decIdx: Int, expIdx: Int): Signal = {
    if (suppress)
      Signal.Continue
    else
      super.num(s, decIdx, expIdx)
  }

  override def str(s: CharSequence): Signal = {
    if (suppress)
      Signal.Continue
    else
      super.str(s)
  }

  // TODO suppress nestMap?
  override def nestMap(pathComponent: CharSequence): Signal = {
    if (atFocus() && !underFocus) {
      if (pivoted) {
        renest(cursor)
        pivotedTwice = true
      }

      val back = if (idStatus eq IdOnly) {
        super.str(pathComponent)
        suppress = true
        Signal.SkipColumn
      } else if (idStatus eq IncludeId) {
        super.nestArr()
        super.str(pathComponent)
        super.unnest()
        super.nestArr()
        Signal.Continue
      } else {
        Signal.Continue
      }

      underFocus = true
      pivoted = true

      back
    } else {
      super.nestMap(pathComponent)
    }
  }

  override def nestArr(): Signal = {
    if (atFocus() && !underFocus) {
      if (pivoted) {
        renest(cursor)
        pivotedTwice = true
      }

      val back = if (idStatus eq IdOnly) {
        super.num(focusedArrayIndex.toString, -1, -1)
        suppress = true
        Signal.SkipColumn
      } else if (idStatus eq IncludeId) {
        super.nestArr()
        super.num(focusedArrayIndex.toString, -1, -1)
        super.unnest()
        super.nestArr()
        Signal.Continue
      } else {
        Signal.Continue
      }

      underFocus = true
      pivoted = true

      back
    } else {
      super.nestArr()
    }
  }

  override def nestMeta(pathComponent: CharSequence): Signal = {
    if (atFocus() && !underFocus) {
      if (pivoted) {
        renest(cursor)
        pivotedTwice = true
      }

      val back = if (idStatus eq IdOnly) {
        super.str(pathComponent)
        suppress = true
        Signal.SkipColumn
      } else if (idStatus eq IncludeId) {
        super.nestArr()
        super.str(pathComponent)
        super.unnest()
        super.nestArr()
        Signal.Continue
      } else {
        Signal.Continue
      }

      underFocus = true
      pivoted = true

      back
    } else {
      super.nestMeta(pathComponent)
    }
  }

  override def unnest(): Signal = {
    /*
     * When we come into this function with IncludeId, we're going to be one position
     * deeper than the focus (specifically, the second array index). We need to account
     * for this in our testing here. Note that InclueId is the only wrapping scenario,
     * which is why we need to special-case it.
     */
    if ((atFocus() || ((idStatus eq IncludeId) && atFocusPlus1())) && underFocus) {
      if (idStatus eq IncludeId) {
        super.unnest()   // get out of the second array component
      }

      underFocus = false
      suppress = false

      focusedArrayIndex += 1

      Signal.Continue
    } else {
      super.unnest()
    }
  }

  override def finishRow(): Unit = {
    if (pivotedTwice) {
      @tailrec
      def unnestAll(cursor: List[CPathNode]): Unit = cursor match {
        case _ :: tail =>
          super.unnest()
          unnestAll(cursor)

        case Nil =>
          super.finishRow()
      }

      unnestAll(cursor)
    } else if (!scalarSkip) {
      super.finishRow()
    }

    pivoted = false
    pivotedTwice = false
    scalarSkip = false

    focusedArrayIndex = 0
  }

  // we just hit the focus and nested a non-first time
  private def renest(cursor: List[CPathNode]): Unit = cursor match {
    case hd :: tail =>
      super.unnest()
      renest(tail)

      hd match {
        case CPathField(field) => super.nestMap(field)
        case CPathIndex(_) => super.nestArr()
        case CPathMeta(field) => super.nestMeta(field)
        case c => sys.error(s"impossible component $c")
      }

      ()

    case Nil => super.finishRow()
  }

  private def atFocus(): Boolean = cursor == rfocus

  private def atFocusPlus1(): Boolean = cursor == rfocusPlus1
}

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

package quasar.qsu

import slamdata.Predef.{None, Option, Some, Symbol}

import quasar.qscript.{
  Center,
  HoleF,
  LeftSide,
  LeftSideF,
  LeftSide3,
  RecFreeS,
  RightSide,
  RightSideF,
  RightSide3
}
import scalaz.syntax.bind._

/** Inlines nullary mappable expressions into AutoJoin combiners, reducing or
  * eliminating the AutoJoin.
  */
final class InlineNullary[T[_[_]]] private () extends QSUTTypes[T] {
  import QSUGraph.Extractors._
  import RecFreeS._

  def apply(graph: QSUGraph): QSUGraph =
    graph rewrite {
      case g @ AutoJoin2(left, right, combine) =>
        (nullary(left), nullary(right)) match {
          case (Some((u, l)), Some((_, r))) =>
            val fm = combine flatMap {
              case LeftSide => l
              case RightSide => r
            }
            g.overwriteAtRoot(QSU.Map(u, fm.asRec))

          case (Some((_, l)), None) =>
            val fm = combine flatMap {
              case LeftSide => l
              case RightSide => HoleF[T]
            }
            g.overwriteAtRoot(QSU.Map(right.root, fm.asRec))

          case (None, Some((_, r))) =>
            val fm = combine flatMap {
              case LeftSide => HoleF[T]
              case RightSide => r
            }
            g.overwriteAtRoot(QSU.Map(left.root, fm.asRec))

          case (None, None) => g
        }

      case g @ AutoJoin3(left, center, right, combine) =>
        (nullary(left), nullary(center), nullary(right)) match {
          case (Some((u, l)), Some((_, c)), Some((_, r))) =>
            val fm = combine flatMap {
              case LeftSide3 => l
              case Center => c
              case RightSide3 => r
            }
            g.overwriteAtRoot(QSU.Map(u, fm.asRec))

          case (Some((_, l)), Some((_, c)), None) =>
            val fm = combine flatMap {
              case LeftSide3 => l
              case Center => c
              case RightSide3 => HoleF[T]
            }
            g.overwriteAtRoot(QSU.Map(right.root, fm.asRec))

          case (Some((_, l)), None, Some((_, r))) =>
            val fm = combine flatMap {
              case LeftSide3 => l
              case Center => HoleF[T]
              case RightSide3 => r
            }
            g.overwriteAtRoot(QSU.Map(center.root, fm.asRec))

          case (Some((_, l)), None, None) =>
            val jf = combine flatMap {
              case LeftSide3 => l >> LeftSideF[T]
              case Center => LeftSideF[T]
              case RightSide3 => RightSideF[T]
            }
            g.overwriteAtRoot(QSU.AutoJoin2(center.root, right.root, jf))

          case (None, Some((_, c)), Some((_, r))) =>
            val fm = combine flatMap {
              case LeftSide3 => HoleF[T]
              case Center => c
              case RightSide3 => r
            }
            g.overwriteAtRoot(QSU.Map(left.root, fm.asRec))

          case (None, Some((_, c)), None) =>
            val jf = combine flatMap {
              case LeftSide3 => LeftSideF[T]
              case Center => c >> LeftSideF[T]
              case RightSide3 => RightSideF[T]
            }
            g.overwriteAtRoot(QSU.AutoJoin2(left.root, right.root, jf))

          case (None, None, Some((_, r))) =>
            val jf = combine flatMap {
              case LeftSide3 => LeftSideF[T]
              case Center => RightSideF[T]
              case RightSide3 => r >> LeftSideF[T]
            }
            g.overwriteAtRoot(QSU.AutoJoin2(left.root, center.root, jf))

          case (None, None, None) => g
        }
    }

  private val QSU = QScriptUniform

  private val nullary: QSUGraph => Option[(Symbol, FreeMap)] = {
    case Map(u @ Unreferenced(), f) => Some((u.root, f.linearize))
    case _ => None
  }
}

object InlineNullary {
  def apply[T[_[_]]](graph: QSUGraph[T]): QSUGraph[T] =
    (new InlineNullary[T])(graph)
}

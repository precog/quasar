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

package quasar.mimir

import quasar.mimir.MimirCake.{Cake, SortState}
import scalaz._

trait MimirRepr {
  type P <: Cake

  // Scala is afraid to infer this but it must be valid,
  // because Cake <: Singleton
  val sing: Leibniz[⊥, Cake, P, P.type]

  val P: P
  val table: P.Table
  val lastSort: Option[SortState[P.trans.TransSpec1]]

  // these casts can't crash, because `Precog` is final.
  // however they're unsafe because they could lead to sharing cakes
  // between state threads.
  def unsafeMerge(other: MimirRepr): MimirRepr.Aux[MimirRepr.this.P.type] =
    other.asInstanceOf[MimirRepr.Aux[MimirRepr.this.P.type]]

  def unsafeMergeTable(other: MimirRepr#P#Table): MimirRepr.this.P.Table =
    other.asInstanceOf[MimirRepr.this.P.Table]

  // this is totally safe because `Precog` is final *and* `TransSpec` can't reference cake state.
  def mergeTS1(other: or.P.trans.TransSpec1 forSome { val or: MimirRepr })
      : MimirRepr.this.P.trans.TransSpec1 =
    other.asInstanceOf[MimirRepr.this.P.trans.TransSpec1]

  def map(f: P.Table => P.Table): MimirRepr.Aux[MimirRepr.this.P.type] =
    MimirRepr(P)(f(table))
}

object MimirRepr {
  type Aux[P0 <: Cake] = MimirRepr { type P = P0 }

  def apply(P0: Precog)(table0: P0.Table): MimirRepr.Aux[P0.type] =
    new MimirRepr {
      type P = P0.type

      val sing: Leibniz[P, P, P, P] = Leibniz.refl[P]

      val P: P0.type = P0
      val table: P.Table = table0
      val lastSort: Option[SortState[P0.trans.TransSpec1]] = None
    }

  def withSort(P0: Precog)(table0: P0.Table)(lastSort0: Option[SortState[P0.trans.TransSpec1]])
      : MimirRepr.Aux[P0.type] =
    new MimirRepr {
      type P = P0.type

      val sing: Leibniz[P, P, P, P] = Leibniz.refl[P]

      val P: P0.type = P0
      val table: P.Table = table0
      val lastSort: Option[SortState[P0.trans.TransSpec1]] = lastSort0
    }

  // witness that all cakes have a singleton type for P
  def single[P0 <: Cake](src: MimirRepr.Aux[P0]): MimirRepr.Aux[src.P.type] =
    src.sing.subst[MimirRepr.Aux](src)
}

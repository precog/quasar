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

package quasar.qscript.provenance

import slamdata.Predef._
import quasar.fp.ski.ι

import matryoshka._
import scalaz._, Scalaz._

trait Dimension[D, I, P] {
  val prov: Prov[D, I, P]

  import prov._

  /** Returns the `JoinKeys` describing the autojoin of the two dimension stacks. */
  def autojoinKeys(ls: Dimensions[P], rs: Dimensions[P])(implicit D: Equal[D]): JoinKeys[I] =
    ls.reverse.fzipWith(rs.reverse)(joinKeys).fold

  /** The empty dimension stack. */
  val empty: Dimensions[P] =
    IList[P]()

  def canonicalize(ds: Dimensions[P])(implicit eqD: Equal[D], eqI: Equal[I]): Dimensions[P] =
    ds.map(normalize)

  /** Updates the dimensional stack by sequencing a new dimension from value
    * space with the current head dimension.
    */
  def flatten(id: I, ds: Dimensions[P]): Dimensions[P] =
    nest(lshift(id, ds))

  /** Joins two dimensions into a single dimension stack, starting from the base. */
  def join(ls: Dimensions[P], rs: Dimensions[P])(implicit eqD: Equal[D], eqI: Equal[I]): Dimensions[P] =
    canonicalize(alignRtoL(ls, rs)(ι, ι, both(_, _)))

  /** Shifts the dimensional stack by pushing a new dimension from value space
    * onto the stack.
    */
  def lshift(id: I, ds: Dimensions[P]): Dimensions[P] =
    value(id) :: ds

  /** Sequences the first and second dimensions. */
  def nest(ds: Dimensions[P]): Dimensions[P] =
    ds.toNel.fold(ds)(nel => extend[P](thenn(_, _), nel.head)(nel.tail))

  /** Project a static key/index from maps and arrays. */
  def project(field: D, ds: Dimensions[P]): Dimensions[P] =
    extend[P](thenn(_, _), proj(field))(ds)

  /** Reduces the dimensional stack by peeling off the current dimension. */
  def reduce(ds: Dimensions[P]): Dimensions[P] =
    ds drop 1

  /** Collapses all dimensions into a single one. */
  def squash(ds: Dimensions[P]): Dimensions[P] =
    ds.toNel.fold(ds)(nel => IList(nel.foldRight1(thenn(_, _))))

  /** Swaps the dimensions at the nth and mth indices. */
  def swap(idxN: Int, idxM: Int, ds: Dimensions[P]): Dimensions[P] = {
    val n = if (idxN < idxM) idxN else idxM
    val m = if (idxM > idxN) idxM else idxN

    val swapped = for {
      z0  <- ds.toZipper
      z0n <- z0.move(n)
      vn  =  z0n.focus
      z0m <- z0n.move(m - n)
      vm  =  z0m.focus
      z1  =  z0m.update(vn)
      z1n <- z1.move(n - m)
      z2  =  z1n.update(vm)
    } yield z2.toIList

    swapped getOrElse ds
  }

  /** Unions the two dimensions into a single dimensional stack, starting from the base. */
  def union(ls: Dimensions[P], rs: Dimensions[P])(implicit eqD: Equal[D], eqI: Equal[I]): Dimensions[P] =
    canonicalize(alignRtoL(ls, rs)(oneOf(_, nada()), oneOf(nada(), _), oneOf(_, _)))

  ////

  private def alignRtoL[A]
      (ls: Dimensions[P], rs: Dimensions[P])
      (ths: P => A, tht: P => A, bth: (P, P) => A)
      : Dimensions[A] =
    ls.reverse.alignWith(rs.reverse)(_.fold(ths, tht, bth)).reverse

  private def extend[A](f: (A, A) => A, a: A): IList[A] => IList[A] =
    _.toNel.fold(IList(a))(nel => f(a, nel.head) :: nel.tail)
}

object Dimension {
  def apply[D, I, P](prov0: Prov[D, I, P]): Dimension[D, I, P] =
    new Dimension[D, I, P] { val prov = prov0 }
}

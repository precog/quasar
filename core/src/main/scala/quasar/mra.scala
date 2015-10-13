/*
 * Copyright 2014 - 2015 SlamData Inc.
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

package quasar

import quasar.Predef._
import quasar.fs._

import scalaz._; import Scalaz._

object MRA {
  // foo[*].bar + foo[*].baz
  // foo.bar[*] + foo.baz[*]
  // foo[*] + baz[*]
  //


  import LogicalPlan._

  final case class Dims private (contracts: List[DimContract], id: DimId, expands: List[DimExpand]) {
    def size = contracts.length + (if (value) 0 else 1) + expands.length

    def value = squash.id == DimId.Value

    def contract: Dims = copy(contracts = DimContract :: contracts)

    def flatten: Dims = copy(id = DimId.Flatten(id))

    def expand: Dims = copy(expands = DimExpand :: expands)

    def contracted = contracts.length > 0

    def squash = copy(contracts = Nil, id = id.squash, expands = Nil)

    def squashed = squash.id.simplify == id.simplify

    def field(name: String): Dims = copy(id = DimId.ObjProj(id, name))

    def index(idx: Long): Dims = copy(id = DimId.ArrProj(id, idx))

    def identified = !value

    def expanded = expands.length > 0

    def ++ (that: Dims) = Dims(
      if (contracts.length > that.contracts.length) contracts else that.contracts,
      (this.id & that.id).simplify,
      if (expands.length > that.expands.length) expands else that.expands
    )

    def aggregate: Dims = {
      if (expands.length > 0) copy(expands = expands.drop(1))
      else if (id != DimId.Value) copy(id = DimId.Value)
      else if (contracts.length > 0) copy(contracts = contracts.drop(1))
      else this
    }

    def simplify: Dims = copy(id = id.simplify)
  }
  object Dims {
    def Value = id(DimId.Value)

    def set(p: Path, ps: Path*) = new Dims(Nil, ps.map(DimId.Source.apply).foldLeft[DimId](DimId.Source(p))(_ & _), Nil)

    def combineAll(list: List[Dims]): Dims = list match {
      case Nil => Value
      case head :: tail => tail.foldLeft(head)(_ ++ _).simplify
    }

    def id(id: DimId) = new Dims(Nil, id, Nil)
  }

  sealed trait DimId {
    import DimId._

    def simplify: DimId = {
      val s = canonicalize

      // Remove all subsumptions
      val s2 = Set(s.tail.foldLeft[List[DimId]](s.head :: Nil) {
        case (acc, d) =>
          if (acc.exists(_ subsumes d)) acc
          else d :: acc.filterNot(d subsumes _)
      }: _*)

      s2.foldMap(ι)(DimId.DimIdMonoid)
    }

    def maxSize = size0(_.max)

    def minSize = size0(_.min)

    private def size0(f: Set[Int] => Int): Int = this match {
      case ArrProj(on, _) => 1 + on.size0(f)
      case ObjProj(on, _) => 1 + on.size0(f)
      case Flatten(on)    => 1 + on.size0(f)
      case p @ Product(_, _) => f(p.flattenSet.map(_.size0(f)))
      case _ => 1
    }

    /**
     * Flattens all the product elements to top-level elements of a set
     */
    def canonicalize: Set[DimId] = this match {
      case ArrProj(on, index) => on.canonicalize.map(ArrProj(_, index))
      case ObjProj(on, name)  => on.canonicalize.map(ObjProj(_, name))
      case Flatten(on)        => on.canonicalize.map(Flatten(_))
      case Product(l, r)      => l.canonicalize ++ r.canonicalize

      case x => Set(x)
    }

    def subsumes(that: DimId): Boolean = {
      val s1 = this.canonicalize
      val s2 = that.canonicalize

      s2.forall(v2 => s1.exists(v1 => v1.subsumes0(v2)))
    }

    private def subsumes0(that: DimId): Boolean = (this, that) match {
      case (x, y) if (x == y) => true

      case (_, Value) => true

      case (ArrProj(o, _), _) => o.subsumes(that)
      case (ObjProj(o, _), _) => o.subsumes(that)
      case (Flatten(o), _)    => o.subsumes(that)

      case _ => false
    }

    def intersect(that: DimId): Option[DimId] = {
      val s1 = this.canonicalize
      val s2 = that.canonicalize

      val rez = for {
        v1 <- s1
        v2 <- s2
      } yield v1.intersect0(v2)

      rez.toList.foldMap(ι)
    }

    private def intersect0(that: DimId): Option[DimId] = (this, that) match {
      case (x, y) if (x == y) => Some(x)

      case (ArrProj(o, _), _) => o.intersect(that) orElse (that.intersect(o))
      case (ObjProj(o, _), _) => o.intersect(that) orElse (that.intersect(o))
      case (Flatten(o), _)    => o.intersect(that) orElse (that.intersect(o))

      case _ => None
    }

    def squash: DimId = this match {
      case ArrProj(on, _) => on.squash
      case ObjProj(on, _) => on.squash
      case Flatten(on)    => on.squash
      case Product(l, r)  => Product.product(l.squash, r.squash)

      case x => x
    }

    def id: String = this match {
      case Value => "<value>"
      case Source(path) => path.pathname
      case ArrProj(on, index) => on.id + ("[" + index + "]")
      case ObjProj(on, name)  => on.id + ("{" + name  + "}")
      case Flatten(on) => on.id + "(*)"
      case Product(l, r) => "(" + l.id + " & " + r.id + ")"
    }

    def index(idx: Long): DimId = ArrProj(this, idx)

    def field(name: String): DimId = ObjProj(this, name)

    def flatten: DimId = Flatten(this)

    def & (that: DimId): DimId = Product(this, that)

    override def toString = id
  }
  object DimId {
    implicit val DimIdMonoid = new Monoid[DimId] {
      def zero = Value

      def append(v1: DimId, v2: => DimId): DimId = if (v1 == Value) v2 else if (v2 == Value) v1 else v1 & v2
    }
    final case object Value extends DimId
    final case class Source(path: Path) extends DimId
    final case class ArrProj(on: DimId, index: Long) extends DimId
    final case class ObjProj(on: DimId, name: String) extends DimId
    final case class Flatten(on: DimId) extends DimId
    final case class Product(left: DimId, right: DimId) extends DimId {
      def flattenProduct: List[DimId] = {
        def flatten0(v: DimId): List[DimId] = v match {
          case Product(l, r) => flatten0(l) ++ flatten0(r)
          case x => x :: Nil
        }

        flatten0(this)
      }

      def flattenSet: Set[DimId] = Set(flattenProduct: _*)

      override def hashCode = flattenSet.hashCode

      override def equals(that: scala.Any) = that match {
        case that @ Product(_, _) => this.flattenSet == that.flattenSet
        case _ => false
      }
    }
    object Product {
      def product(p: DimId, ps: DimId*): DimId = (Set(ps: _*) - p).foldLeft(p)(_ & _)
    }
  }

  sealed trait DimContract
  final case object DimContract extends DimContract

  sealed trait DimExpand
  final case object DimExpand extends DimExpand

  val dimsƒ: LogicalPlan[Dims] => Dims = {
    case ReadF(path) => Dims.set(path)
    case ConstantF(_) => Dims.Value
    case InvokeF(func, args) =>
      val d = Dims.combineAll(args)

      func match {
        case Mapping(_, _, _, _, _, _)        => d
        case Expansion(_, _, _, _, _, _)      => d.expand
        case ExpansionFlat(_, _, _, _, _, _)  => d.flatten
        case Reduction(_, _, _, _, _, _)      => d.aggregate
        case Sifting(_, _, _, _, _, _)        => d
        case Transformation(_, _, _, _, _, _) => d
        case Squashing(_, _, _, _, _, _)      => d.squash
      }
    case FreeF(_) => Dims.Value
    case LetF(_, _, in) => in
  }
}

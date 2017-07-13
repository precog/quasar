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

package quasar.tpe

import slamdata.Predef._
import quasar.contrib.matryoshka._
import quasar.ejson
import quasar.ejson.{CommonEJson => C, ExtEJson => E, EJson, EncodeEJson, EncodeEJsonK}
import quasar.fp.ski.κ

import scala.Tuple2
import scala.Function.untupled

import algebra.PartialOrder
import algebra.lattice._
import matryoshka.{project => _, _}
import matryoshka.implicits._
import monocle.Prism
import scalaz._, Scalaz._

sealed abstract class TypeF[J, A] extends Product with Serializable

object TypeF extends TypeFInstances {
  import SimpleType._

  final case class Bottom[J, A]()                                        extends TypeF[J, A]
  final case class Top[J, A]()                                           extends TypeF[J, A]
  final case class Simple[J, A](simpleType: SimpleType)                  extends TypeF[J, A]
  final case class Const[J, A](ejson: J)                                 extends TypeF[J, A]
  final case class Arr[J, A](elementsOrLub: IList[A] \/ A)               extends TypeF[J, A]
  final case class Map[J, A](known: IMap[J, A], unknown: Option[(A, A)]) extends TypeF[J, A]
  final case class Union[J, A](fst: A, snd: A, rest: IList[A])           extends TypeF[J, A]

  object Leaf {
    def unapply[J, A](tf: TypeF[J, A]): Option[SimpleType \/ J] =
      leaf.getOption(tf)
  }

  object Unioned {
    def unapply[J, A](tf: TypeF[J, A]): Option[NonEmptyList[A]] =
      union[J, A].getOption(tf) map {
        case (x, y, zs) => NonEmptyList.nel(x, y :: zs)
      }
  }


  // Prisms

  def bottom[J, A]: Prism[TypeF[J, A], Unit] =
    Prism.partial[TypeF[J, A], Unit] {
      case Bottom() => ()
    } (κ(Bottom()))

  def top[J, A]: Prism[TypeF[J, A], Unit] =
    Prism.partial[TypeF[J, A], Unit] {
      case Top() => ()
    } (κ(Top()))

  def simple[J, A]: Prism[TypeF[J, A], SimpleType] =
    Prism.partial[TypeF[J, A], SimpleType] {
      case Simple(t) => t
    } (Simple(_))

  def const[J, A]: Prism[TypeF[J, A], J] =
    Prism.partial[TypeF[J, A], J] {
      case Const(j) => j
    } (Const(_))

  def arr[J, A]: Prism[TypeF[J, A], IList[A] \/ A] =
    Prism.partial[TypeF[J, A], IList[A] \/ A] {
      case Arr(asOrLub) => asOrLub
    } (Arr(_))

  def map[J, A]: Prism[TypeF[J, A], (IMap[J, A], Option[(A, A)])] =
    Prism.partial[TypeF[J, A], (IMap[J, A], Option[(A, A)])] {
      case Map(kn, unk) => (kn, unk)
    } { case (k, u) => Map(k, u) }

  def coproduct[J, A]: Prism[TypeF[J, A], (A, A)] =
    Prism.partial[TypeF[J, A], (A, A)] {
      case Union(x, y, INil()) => (x, y)
    } { case (x, y) => Union(x, y, INil()) }

  def union[J, A]: Prism[TypeF[J, A], (A, A, IList[A])] =
    Prism.partial[TypeF[J, A], (A, A, IList[A])] {
      case Union(x, y, zs) => (x, y, zs)
    } { case (x, y, zs) => Union(x, y, zs) }

  def leaf[J, A]: Prism[TypeF[J, A], SimpleType \/ J] =
    Prism[TypeF[J, A], SimpleType \/ J](
      tf => simple[J, A].getOption(tf).map(_.left) orElse const[J, A].getOption(tf).map(_.right))(
      _.fold(simple[J, A](_), const[J, A](_)))


  // Functions + folds

  /** Returns the greatest subtype of both types. */
  object glb {
    def apply[J] = new PartiallyApplied[J]
    final class PartiallyApplied[J] {
      def apply[T](x: T, y: T)(
        implicit
        J : Order[J],
        TR: Recursive.Aux[T, TypeF[J, ?]],
        TC: Corecursive.Aux[T, TypeF[J, ?]],
        JR: Recursive.Aux[J, EJson],
        JC: Corecursive.Aux[J, EJson]
      ): T = {
        import normalization._

        val normArg: T => T =
          _.transCata[T](
                reduceToBottom[J, T]
            >>> normalizeEJson[J, T]
            >>> lowerConst[J, T]
            >>> coalesceUnion[J, T]
            >>> reduceToTop[J, T]
            >>> elideBottom[J, T]
          )

        (x, y).ghylo[Id, T \/ ?](
          distCata[TypeF[J, ?]],
          distApo[T, TypeF[J, ?]],
          normalization.normalizeƒ[J, T] >>> (_.embed),
          glbƒ[J, T] <<< normArg.product)
      }
    }
  }

  /** Unfold a pair of types into their greatest lower-bound. */
  def glbƒ[J: Order, T](
    implicit
    TR: Recursive.Aux[T, TypeF[J, ?]],
    TC: Corecursive.Aux[T, TypeF[J, ?]],
    JR: Recursive.Aux[J, EJson]
  ): GCoalgebra[T \/ ?, TypeF[J, ?], (T, T)] = {
    type LR  = (T, T)
    type OUT = TypeF[J, T \/ LR]
    val T    = top[J, T]().embed
    val ⊥    = bottom[J, T]().embed

    val arrayGlb: ((IList[T] \/ T, IList[T] \/ T)) => IList[LR] \/ LR = {
      case (\/-( x), \/-( y)) => (x, y).right
      case (\/-( x), -\/(ys)) => ys.map((x, _)).left
      case (-\/(xs), \/-( y)) => xs.map((_, y)).left
      case (-\/(xs), -\/(ys)) => xs.alignWith(ys)(_.fold((_, T), (T, _), (_, _))).left
    }

    // NB: We could do a bit better in the scenario where X includes known keys
    //     not present in Y and Y has unknown keys by taking the glb of the
    //     product of the keys of (X - Y) and Y's unknown key type.
    //
    //     To do this we'd need a way to specify a product or, analogously, the
    //     glb of more than two types.
    def mapGlb(xs: IMap[J, T], ux: Option[(T, T)], ys: IMap[J, T], uy: Option[(T, T)]): TypeF[J, LR] =
      map[J, LR](
        xs.alignWith(ys)(_.fold(
          x      => (x, uy ? ⊥ | T),
          y      => (ux ? ⊥ | T, y),
          (x, y) => (x, y)
        )),
        ux.alignWith(uy)(_.fold(
          _.umap((_, T)),
          _.umap((T, _)),
          untupled({ case ((xk, xv), (yk, yv)) => ((xk, yk), (xv, yv)) })
        )))

    _.umap(_.project) match {
      case (        Bottom(),                _)                   => bottom()
      case (               _,         Bottom())                   => bottom()
      case (           Top(),                y)                   => y map (_.left)
      case (               x,            Top())                   => x map (_.left)
      case (       Simple(x),        Simple(y)) if x ≟ y          => simple(x)
      case (        Const(x),         Const(y)) if x ≟ y          => const(x)
      case (       Simple(s),         Const(j)) if isSimply(j, s) => const(j)
      case (        Const(j),        Simple(s)) if isSimply(j, s) => const(j)
      case (Arr(-\/(INil())),           Arr(y))                   => arr[J, T](y) map (_.left)
      case (          Arr(x), Arr(-\/(INil())))                   => arr[J, T](x) map (_.left)
      case (          Arr(x),           Arr(y))                   => arr[J, LR](arrayGlb((x, y))) map (_.right)
      case (   Map(xs, None),      Map(ys, uy)) if xs.isEmpty     => map[J, T](ys, uy) map (_.left)
      case (     Map(xs, ux),    Map(ys, None)) if ys.isEmpty     => map[J, T](xs, ux) map (_.left)
      case (     Map(xs, ux),      Map(ys, uy))                   => mapGlb(xs, ux, ys, uy) map (_.right)
      case ( Union(a, b, cs),                y)                   => union[J, T](a, b, cs) map ((_, y.embed).right)
      case (               x,  Union(a, b, cs))                   => union[J, T](a, b, cs) map ((x.embed, _).right)
      case _                                                      => bottom()
    }
  }

  /** Returns whether two types have identical structure. */
  object identical {
    def apply[J] = new PartiallyApplied[J]
    final class PartiallyApplied[J] {
      def apply[T](a: T, b: T)(implicit J: Equal[J], T: Recursive.Aux[T, TypeF[J, ?]]): Boolean =
        Recursive.equal[T, TypeF[J, ?]](traverse[J], T, structuralEqual[J]).equal(a, b)
    }
  }

  /** Whether a type is `bottom`. */
  object isBottom {
    def apply[J] = new PartiallyApplied[J]
    final class PartiallyApplied[J] {
      def apply[T](t: T)(implicit T: Recursive.Aux[T, TypeF[J, ?]]): Boolean =
        project[T, TypeF[J, ?]].composePrism(bottom[J, T]).exist(κ(true))(t)
    }
  }

  /** Whether `x` is a subtype of `y`. */
  object isSubtypeOf {
    def apply[J] = new PartiallyApplied[J]
    final class PartiallyApplied[J] {
      def apply[T](x: T, y: T)(
        implicit
        J : Order[J],
        TR: Recursive.Aux[T, TypeF[J, ?]],
        TC: Corecursive.Aux[T, TypeF[J, ?]],
        JR: Recursive.Aux[J, EJson],
        JC: Corecursive.Aux[J, EJson]
      ): Boolean =
        subtypingPartialOrder[J, T].lteqv(x, y)
    }
  }

  /** Whether a type is `top`. */
  object isTop {
    def apply[J] = new PartiallyApplied[J]
    final class PartiallyApplied[J] {
      def apply[T](t: T)(implicit T: Recursive.Aux[T, TypeF[J, ?]]): Boolean =
        project[T, TypeF[J, ?]].composePrism(top[J, T]).exist(κ(true))(t)
    }
  }

  /** Returns the smallest supertype of both types. */
  object lub {
    def apply[J] = new PartiallyApplied[J]
    final class PartiallyApplied[J] {
      def apply[T](x: T, y: T)(
        implicit
        J : Order[J],
        TR: Recursive.Aux[T, TypeF[J, ?]],
        TC: Corecursive.Aux[T, TypeF[J, ?]],
        JR: Recursive.Aux[J, EJson],
        JC: Corecursive.Aux[J, EJson]
      ): T =
        normalization.normalize[J](coproduct[J, T](x, y).embed)
    }
  }

  /** Returns the `PrimaryType` of the given type, if exists. */
  def primary[J](tf: TypeF[J, _])(
    implicit J: Recursive.Aux[J, EJson]
  ): Option[PrimaryType] = tf match {
    case Simple(s)                         => some(s.left)
    case Const(j)                          => some(primaryTypeOf(j))
    case Arr(_)                            => some(CompositeType.Arr.right)
    case Map(_, _)                         => some(CompositeType.Map.right)
    case Bottom() | Top() | Union(_, _, _) => none
  }

  ////

  /** Returns whether the given EJson is of the specified `SimpleType`. */
  private def isSimply[J](j: J, s: SimpleType)(
    implicit J: Recursive.Aux[J, EJson]
  ): Boolean =
    simpleTypeOf(j) exists (_ ≟ s)

  /** Returns the structural union of a non-empty foldable of types. */
  private[tpe] object unionOf {
    def apply[J] = new PartiallyApplied[J]
    final class PartiallyApplied[J] {
      def apply[F[_]: Foldable1, T](ts: F[T])(
        implicit TC: Corecursive.Aux[T, TypeF[J, ?]]
      ): T =
        ts.toNel match {
          case NonEmptyList(x, ICons(y, zs)) => union[J, T](x, y, zs).embed
          case NonEmptyList(x,       INil()) => x
        }
    }
  }
}

private[quasar] sealed abstract class TypeFInstances {
  import TypeF._

  def boundedDistributiveLattice[J: Order, T](
    implicit
    TR: Recursive.Aux[T, TypeF[J, ?]],
    TC: Corecursive.Aux[T, TypeF[J, ?]],
    JR: Recursive.Aux[J, EJson],
    JC: Corecursive.Aux[J, EJson]
  ): BoundedDistributiveLattice[T] =
    new BoundedDistributiveLattice[T] {
      def meet(l: T, r: T): T = glb[J](l, r)
      def join(l: T, r: T): T = lub[J](l, r)
      val one: T = TC.embed(top())
      val zero: T = TC.embed(bottom())
    }

  def subtypingPartialOrder[J: Order, T](
    implicit
    TR: Recursive.Aux[T, TypeF[J, ?]],
    TC: Corecursive.Aux[T, TypeF[J, ?]],
    JR: Recursive.Aux[J, EJson],
    JC: Corecursive.Aux[J, EJson]
  ): PartialOrder[T] =
    new PartialOrder[T] {
      def partialCompare(x: T, y: T): Double = {
        val normX = normalization.normalize[J](x)
        val normY = normalization.normalize[J](y)

        if (identical[J](normX, normY))
          0.0
        else
          glb[J](normX, normY) match {
            case z if identical[J](normX, z) => -1.0
            case z if identical[J](normY, z) =>  1.0
            case _                           => Double.NaN
          }
      }
    }

  /** NB: This is structural equality and ignores semantics, for most use-cases
    *     the equality implied by the subtyping partial order is more useful.
    */
  def structuralEqual[J: Equal]: Delay[Equal, TypeF[J, ?]] =
    new Delay[Equal, TypeF[J, ?]] {
      def apply[A](eql: Equal[A]): Equal[TypeF[J, A]] = {
        implicit val eqlA: Equal[A] = eql
        Equal.equal((x, y) => (x, y) match {
          case (Unioned(xs), Unioned(ys)) => equalAsSets(xs, ys)
          case _                          => generic(x) ≟ generic(y)
        })
      }

      def equalAsSets[F[_]: Foldable, A: Equal](xs: F[A], ys: F[A]): Boolean =
        xs.all(ys element _) && ys.all(xs element _)

      def generic[A](tf: TypeF[J, A]) = (
        bottom[J, A].getOption(tf),
         const[J, A].getOption(tf),
        simple[J, A].getOption(tf),
           arr[J, A].getOption(tf),
           map[J, A].getOption(tf),
         union[J, A].getOption(tf),
           top[J, A].getOption(tf)
      )
    }

  implicit def encodeEJsonK[A](implicit A: EncodeEJson[A]): EncodeEJsonK[TypeF[A, ?]] =
    new EncodeEJsonK[TypeF[A, ?]] {
      def encodeK[J](implicit J: Corecursive.Aux[J, EJson]): Algebra[TypeF[A, ?], J] = {
        case Bottom()        => tlabel("bottom")
        case Top()           => tlabel("top")
        case Simple(s)       => tlabel(SimpleType.name(s))
        case Const(a)        => tof("const", A.encode[J](a))
        case Union(x, y, zs) => tof("sum", C(ejson.arr((x :: y :: zs).toList)).embed)

        case Arr(a) =>
          tof("array", a.leftMap(js => C(ejson.arr(js.toList)).embed).merge)

        case Map(kvs, unk) =>
          val jjs   = kvs.toList.map(_.leftMap(A.encode[J](_)))
          val other = unk map { case (k, v) => (
            C(ejson.str[J]("other")).embed,
            map1(
              C(ejson.str[J]("keys")).embed   -> k,
              C(ejson.str[J]("values")).embed -> v)
          )}
          tof("map", E(ejson.map(jjs)).embed, other.toList: _*)
      }

      private def map1[J](assoc: (J, J), assocs: (J, J)*)(
        implicit J: Corecursive.Aux[J, EJson]
      ): J =
        E(ejson.map(assoc :: assocs.toList)).embed

      private def tmap[J](v: J, assocs: (J, J)*)(
        implicit J: Corecursive.Aux[J, EJson]
      ): J =
        map1(C(ejson.str[J]("type")).embed -> v, assocs: _*)

      private def tlabel[J](label: String, assocs: (J, J)*)(
        implicit J: Corecursive.Aux[J, EJson]
      ): J =
        tmap(C(ejson.str[J](label)).embed, assocs: _*)

      private def tof[J](label: String, of: J, assocs: (J, J)*)(
        implicit J: Corecursive.Aux[J, EJson]
      ): J =
        tlabel(label, ((C(ejson.str[J]("of")).embed -> of) :: assocs.toList): _*)
    }

  implicit def traverse[J]: Traverse[TypeF[J, ?]] =
    new Traverse[TypeF[J, ?]] {
      def traverseImpl[G[_]: Applicative, A, B](tf: TypeF[J, A])(f: A => G[B]): G[TypeF[J, B]] = tf match {
        case Bottom()        => bottom[J, B]().point[G]
        case Top()           => top[J, B]().point[G]
        case Simple(t)       => simple[J, B](t).point[G]
        case Const(j)        => const[J, B](j).point[G]
        case Arr(eltsOrLub)  => (eltsOrLub bitraverse (_ traverse f, f)) map (arr[J, B](_))
        case Map(kn, unk)    => (kn.traverse(f) |@| UT.traverse(unk)(f))(TypeF.map[J, B](_, _))
        case Union(x, y, zs) => (f(x) |@| f(y) |@| zs.traverse(f))(union[J, B](_, _, _))
      }

      private val UT = Traverse[Option].bicompose[Tuple2].uTraverse
    }

  implicit def show[J: Show]: Delay[Show, TypeF[J, ?]] =
    new Delay[Show, TypeF[J, ?]] {
      def apply[A](show: Show[A]): Show[TypeF[J, A]] = {
        implicit val showA: Show[A] = show

        def showJ(j: J): Cord =
          Cord("`") ++ j.show ++ Cord("`")

        def showKnown(kn: IMap[J, A]): Cord =
          kn.toList map {
            case (j, a) => showJ(j) ++ Cord(" : ") ++ a.show
          } intercalate Cord(", ")

        Show.show {
          case Bottom()              => Cord("⊥")
          case Simple(t)             => t.show
          case Const(j)              => showJ(j)
          case Arr(-\/(as))          => as.show
          case Arr(\/-(lub))         => lub.show ++ Cord("[]")
          case Map(kn, Some((k, v))) => Cord("{") ++
                                          showKnown(kn) ++
                                          Cord(" ? ") ++ k.show ++ Cord(" : ") ++ v.show ++
                                        Cord("}")
          case Map(kn, None)         => Cord("{") ++ showKnown(kn) ++ Cord("}")
          case Unioned(xs)           => Cord("(") ++ (xs map (_.show) intercalate Cord(" | ")) ++ Cord(")")
          case Top()                 => Cord("⊤")
        }
      }
    }
}

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

package quasar

import slamdata.Predef._
import quasar.RenderTree.make
import quasar.RenderTree.ops._
import quasar.fp._

import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import scalaz._, Scalaz._
import simulacrum.typeclass
import iotaz.{CopK, TListK}

@typeclass trait RenderTree[A] {
  def render(a: A): RenderedTree
}

@SuppressWarnings(Array("org.wartremover.warts.ImplicitConversion"))
object RenderTree extends RenderTreeInstances {
  def make[A](f: A => RenderedTree): RenderTree[A] =
    new RenderTree[A] { def render(v: A) = f(v) }

  /** Always a Terminal, with a fixed type and computed label. */
  def simple[A](nodeType: List[String], f: A => Option[String]): RenderTree[A] =
    new RenderTree[A] { def render(v: A) = Terminal(nodeType, f(v)) }

  /** Derive an instance from `Show[A]`, with a static type; e.g. `Shape(Circle(5))`. */
  def fromShow[A: Show](simpleType: String): RenderTree[A] =
    make[A](v => Terminal(List(simpleType), Some(v.shows)))

  /** Derive an instance from `Show[A]`, where the result is one of a few choices,
    * and suitable as the node's type; e.g. `LeftSide`. Note that the `parentType`
    * is not shown in the usual text rendering. */
  def fromShowAsType[A: Show](parentType: String): RenderTree[A] =
    make[A](v => Terminal(List(v.shows, parentType), None))

  /** Derive a `Show[A]` where RenderTree is defined. */
  def toShow[A: RenderTree]: Show[A] = Show.show(_.render.show)

  def delayFromShow[F[_]: Functor: Foldable](implicit F: Delay[Show, F]) =
    new Delay[RenderTree, F] {
      def apply[A](a: RenderTree[A]) = new RenderTree[F[A]] {
        def render(v: F[A]) =
          NonTerminal(List(v.void.shows), None, v.toList.map(a.render))
      }
    }

  /** For use with `<|`, mostly. */
  def print[A: RenderTree](label: String, a: A): Unit =
    println(label + ":\n" + a.render.shows)

  def recursive[T, F[_]](implicit T: Recursive.Aux[T, F], FD: Delay[RenderTree, F], FF: Functor[F]): RenderTree[T] =
    make(_.cata(FD(RenderTree[RenderedTree]).render))
}

sealed abstract class RenderTreeInstances extends RenderTreeInstances0 {
  implicit def const[A: RenderTree]: Delay[RenderTree, Const[A, ?]] =
    Delay.fromNT(λ[RenderTree ~> DelayedA[A]#RenderTree](_ =>
      make(_.getConst.render)))

  implicit def delay[F[_], A: RenderTree](implicit F: Delay[RenderTree, F]): RenderTree[F[A]] =
    F(RenderTree[A])

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  implicit def free[F[_]: Functor](implicit F: Delay[RenderTree, F]): Delay[RenderTree, Free[F, ?]] =
    Delay.fromNT(λ[RenderTree ~> (RenderTree ∘ Free[F, ?])#λ](rt =>
      make(_.resume.fold(F(free[F].apply(rt)).render, rt.render))))

  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  implicit def cofree[F[_]](implicit F: Delay[RenderTree, F]): Delay[RenderTree, Cofree[F, ?]] =
    Delay.fromNT(λ[RenderTree ~> (RenderTree ∘ Cofree[F, ?])#λ](rt =>
      make(t => NonTerminal(List("Cofree"), None, List(rt.render(t.head), F(cofree(F)(rt)).render(t.tail))))))

  implicit def these[A: RenderTree, B: RenderTree]: RenderTree[A \&/ B] =
    make {
      case \&/.Both(a, b) => NonTerminal(List("\\&/"), "Both".some, List(a.render, b.render))
      case \&/.This(a)    => NonTerminal(List("\\&/"), "This".some, List(a.render))
      case \&/.That(b)    => NonTerminal(List("\\&/"), "That".some, List(b.render))
    }

  implicit def coproduct[F[_], G[_], A](implicit RF: RenderTree[F[A]], RG: RenderTree[G[A]]): RenderTree[Coproduct[F, G, A]] =
    make(_.run.fold(RF.render, RG.render))

  implicit lazy val unit: RenderTree[Unit] =
    make(_ => Terminal(List("()", "Unit"), None))

  implicit def renderTreeT[T[_[_]], F[_]: Functor](implicit T: RenderTreeT[T], F: Delay[RenderTree, F]): RenderTree[T[F]] =
    T.renderTree(F)

  implicit def copKRenderTree[LL <: TListK](implicit M: RenderTreeKMaterializer[LL]): Delay[RenderTree, CopK[LL, ?]] = M.materialize(offset = 0)

  implicit def coproductDelay[F[_], G[_]](implicit RF: Delay[RenderTree, F], RG: Delay[RenderTree, G]): Delay[RenderTree, Coproduct[F, G, ?]] =
    Delay.fromNT(λ[RenderTree ~> DelayedFG[F, G]#RenderTree](ra =>
      make(_.run.fold(RF(ra).render, RG(ra).render))))

  implicit def eitherRenderTree[A, B](implicit RA: RenderTree[A], RB: RenderTree[B]): RenderTree[A \/ B] =
    make {
      case -\/ (a) => NonTerminal("-\\/" :: Nil, None, RA.render(a) :: Nil)
      case \/- (b) => NonTerminal("\\/-" :: Nil, None, RB.render(b) :: Nil)
    }

  implicit def optionRenderTree[A](implicit RA: RenderTree[A]): RenderTree[Option[A]] =
    make {
      case Some(a) => RA.render(a)
      case None    => Terminal("None" :: "Option" :: Nil, None)
    }

  implicit def listRenderTree[A](implicit RA: RenderTree[A]): RenderTree[List[A]] =
    make(v => NonTerminal(List("List"), None, v.map(RA.render)))

  implicit def listMapRenderTree[K: Show, V](implicit RV: RenderTree[V]): RenderTree[ListMap[K, V]] =
    make(v => NonTerminal("Map" :: Nil, None,
      v.toList.map { case (k, v) =>
        NonTerminal("Key" :: "Map" :: Nil, Some(k.shows), RV.render(v) :: Nil)
      }))

  implicit def vectorRenderTree[A](implicit RA: RenderTree[A]): RenderTree[Vector[A]] =
    make(v => NonTerminal(List("Vector"), None, v.map(RA.render).toList))

  implicit lazy val booleanRenderTree: RenderTree[Boolean] =
    RenderTree.fromShow[Boolean]("Boolean")

  implicit lazy val intRenderTree: RenderTree[Int] =
    RenderTree.fromShow[Int]("Int")

  implicit lazy val doubleRenderTree: RenderTree[Double] =
    RenderTree.fromShow[Double]("Double")

  implicit lazy val stringRenderTree: RenderTree[String] =
    RenderTree.fromShow[String]("String")

  implicit lazy val symbolRenderTree: RenderTree[Symbol] =
    RenderTree.fromShow[Symbol]("Symbol")

  implicit def pathRenderTree[B,T,S]: RenderTree[pathy.Path[B,T,S]] =
    // NB: the implicit Show instance in scope here ends up being a circular
    // call, so an explicit reference to pathy's Show is needed.
    make(p => Terminal(List("Path"), pathy.Path.pathShow.shows(p).some))

  implicit def leftTuple4RenderTree[A, B, C, D](implicit RA: RenderTree[A], RB: RenderTree[B], RC: RenderTree[C], RD: RenderTree[D]):
      RenderTree[(((A, B), C), D)] =
    new RenderTree[(((A, B), C), D)] {
      def render(t: (((A, B), C), D)) =
        NonTerminal("tuple" :: Nil, None,
           RA.render(t._1._1._1) ::
            RB.render(t._1._1._2) ::
            RC.render(t._1._2) ::
            RD.render(t._2) ::
            Nil)
    }
}

sealed abstract class RenderTreeInstances0 extends RenderTreeInstances1 {
  implicit def leftTuple3RenderTree[A, B, C](
    implicit RA: RenderTree[A], RB: RenderTree[B], RC: RenderTree[C]
  ): RenderTree[((A, B), C)] =
    new RenderTree[((A, B), C)] {
      def render(t: ((A, B), C)) =
        NonTerminal("tuple" :: Nil, None,
          RA.render(t._1._1) ::
          RB.render(t._1._2) ::
          RC.render(t._2)    ::
          Nil)
    }

  implicit def fix[F[_]: Functor](implicit F: Delay[RenderTree, F]): RenderTree[Fix[F]] =
    RenderTree.recursive

  implicit def mu[F[_]: Functor](implicit F: Delay[RenderTree, F]): RenderTree[Mu[F]] =
    RenderTree.recursive

  implicit def nu[F[_]: Functor](implicit F: Delay[RenderTree, F]): RenderTree[Nu[F]] =
    RenderTree.recursive
}

sealed abstract class RenderTreeInstances1 {
  implicit def tuple2RenderTree[A, B](
    implicit RA: RenderTree[A], RB: RenderTree[B]
  ): RenderTree[(A, B)] =
    make(t => NonTerminal("tuple" :: Nil, None,
      RA.render(t._1) ::
      RB.render(t._2) ::
      Nil))
}

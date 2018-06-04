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

import iotaz.CopK
import matryoshka._
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import monocle.{Lens, PLens}
import scalaz.{Lens => _, PLens => _, _}, BijectionT._, Kleisli._, Liskov._, Scalaz._
import shapeless.{Fin, Nat, Sized, Succ}

sealed abstract class ListMapInstances {
  implicit def seqW[A](xs: Seq[A]): SeqW[A] = new SeqW(xs)
  class SeqW[A](xs: Seq[A]) {
    def toListMap[B, C](implicit ev: A <~< (B, C)): ListMap[B, C] = {
      ListMap(co[Seq, A, (B, C)](ev)(xs) : _*)
    }
  }

  implicit def TraverseListMap[K]:
      Traverse[ListMap[K, ?]] with IsEmpty[ListMap[K, ?]] =
    new Traverse[ListMap[K, ?]] with IsEmpty[ListMap[K, ?]] {
      // FIXME: not sure what is being overloaded here
      @SuppressWarnings(Array("org.wartremover.warts.Overloading"))
      def empty[V] = ListMap.empty[K, V]
      def plus[V](a: ListMap[K, V], b: => ListMap[K, V]) = a ++ b
      def isEmpty[V](fa: ListMap[K, V]) = fa.isEmpty
      override def map[A, B](fa: ListMap[K, A])(f: A => B) = fa.map{case (k, v) => (k, f(v))}
      def traverseImpl[G[_],A,B](m: ListMap[K,A])(f: A => G[B])(implicit G: Applicative[G]): G[ListMap[K,B]] = {
        import G.functorSyntax._
        scalaz.std.list.listInstance.traverseImpl(m.toList)({ case (k, v) => f(v) map (k -> _) }) map (_.toListMap)
      }
    }

  implicit def ListMapEqual[A: Equal, B: Equal]: Equal[ListMap[A, B]] =
    Equal.equalBy(_.toList)
}

trait PartialFunctionOps {
  implicit class PFOps[A, B](self: PartialFunction[A, B]) {
    def |?| [C](that: PartialFunction[A, C]): PartialFunction[A, B \/ C] =
      Function.unlift(v =>
        self.lift(v).fold[Option[B \/ C]](
          that.lift(v).map(\/-(_)))(
          x => Some(-\/(x))))
  }
}

trait JsonOps {
  import argonaut._
  import fp.ski._

  def optional[A: DecodeJson](cur: ACursor): DecodeResult[Option[A]] =
    cur.either.fold(
      κ(DecodeResult(scala.util.Right(None))),
      v => v.as[A].map(Some(_)))

  def orElse[A: DecodeJson](cur: ACursor, default: => A): DecodeResult[A] =
    cur.either.fold(
      κ(DecodeResult(scala.util.Right(default))),
      v => v.as[A]
    )

  def decodeJson[A](text: String)(implicit DA: DecodeJson[A]): String \/ A = \/.fromEither(for {
    json <- Parse.parse(text)
    a <- DA.decode(json.hcursor).result.leftMap { case (exp, hist) => "expected: " + exp + "; " + hist.toString }
  } yield a)


  /* Nicely formatted, order-preserving, single-line. */
  val minspace = PrettyParams(
    "",       // indent
    "", " ",  // lbrace
    " ", "",  // rbrace
    "", " ",  // lbracket
    " ", "",  // rbracket
    "",       // lrbracketsEmpty
    "", " ",  // arrayComma
    "", " ",  // objectComma
    "", " ",  // colon
    true,     // preserveOrder
    false     // dropNullKeys
  )

  /** Nicely formatted, order-preserving, 2-space indented. */
  val multiline = PrettyParams(
    "  ",     // indent
    "", "\n",  // lbrace
    "\n", "",  // rbrace
    "", "\n",  // lbracket
    "\n", "",  // rbracket
    "",       // lrbracketsEmpty
    "", "\n",  // arrayComma
    "", "\n",  // objectComma
    "", " ",  // colon
    true,     // preserveOrder
    false     // dropNullKeys
  )
}

trait DebugOps {
  final implicit class ToDebugOps[A](val self: A) {
    /** Applies some operation to a value and returns the original value. Useful
      * for things like adding debugging printlns in the middle of an
      * expression.
      */
    final def <|(f: A => Unit): A = {
      f(self)
      self
    }
  }
}

package object fp
    extends ListMapInstances
    with PartialFunctionOps
    with JsonOps
    with ProcessOps
    with DebugOps {

  import ski._

  /** An endomorphism is a mapping from a category to itself.
   *  It looks like scalaz already staked out "Endo" for the
   *  lower version.
   */
  type EndoK[F[X]] = scalaz.NaturalTransformation[F, F]

  // TODO generalize this and matryoshka.Delay into
  // `type KleisliK[M[_], F[_], G[_]] = F ~> (M ∘ G)#λ`
  type NTComp[F[X], G[Y]] = scalaz.NaturalTransformation[F, matryoshka.∘[G, F]#λ]

  /** Accept a value (forcing the argument expression to be evaluated for its
    * effects), and then discard it, returning Unit. Makes it explicit that
    * you're discarding the result, and effectively suppresses the
    * "NonUnitStatement" warning from wartremover.
    */
  def ignore[A](a: A): Unit = ()

  def reflNT[F[_]]: F ~> F = NaturalTransformation.refl[F]

  /** `liftM` as a natural transformation
    *
    * TODO: PR to scalaz
    */
  def liftMT[F[_]: Monad, G[_[_], _]: MonadTrans] = λ[F ~> G[F, ?]](_.liftM[G])

  /** `point` as a natural transformation */
  def pointNT[F[_]: Applicative] = λ[Id ~> F](Applicative[F] point _)

  def evalNT[F[_]: Monad, S](initial: S) = λ[StateT[F, S, ?] ~> F](_ eval initial)

  def liftFG[F[_], G[a] <: ACopK[a], A](orig: F[A] => G[A])(implicit F: F :<<: G):
      G[A] => G[A] =
    ftf => F.prj(ftf).fold(ftf)(orig)

  def liftFGM[M[_]: Monad, F[_], G[_], A](orig: F[A] => M[G[A]])(implicit F: F :<: G):
      G[A] => M[G[A]] =
    ftf => F.prj(ftf).fold(ftf.point[M])(orig)

  def liftFGTrans[F[_], G[_], A](prism: PrismNT[G, F])(f: F[A] => Option[G[A]])
      : G[A] => Option[G[A]] =
    ga => prism.get(ga).flatMap(f)


  def liftFF[F[_], G[_], A](orig: F[A] => F[A])(implicit F: F :<: G):
      G[A] => G[A] =
    ftf => F.prj(ftf).fold(ftf)(orig.andThen(F.inj))

  def liftFFCopK[F[_], G[a] <: ACopK[a], A](orig: F[A] => F[A])(implicit F: F :<<: G):
      G[A] => G[A] =
    ftf => F.prj(ftf).fold(ftf)(orig.andThen(F.inj))

  def liftFFTrans[F[_], G[_], A](prism: PrismNT[G, F])(f: F[A] => Option[F[A]])
      : G[A] => Option[G[A]] =
    ga => prism.get(ga).flatMap(f).map(prism.reverseGet(_))

  def liftR[T[_[_]]: BirecursiveT, F[_]: Traverse, G[_]: Traverse](orig: T[F] => T[F])(implicit F: F:<: G):
      T[G] => T[G] =
    tg => prjR[T, F, G](tg).fold(tg)(orig.andThen(injR[T, F, G]))

  def injR[T[_[_]]: BirecursiveT, F[_]: Functor, G[_]: Functor](orig: T[F])(implicit F: F :<: G):
      T[G] =
    orig.transCata[T[G]](F.inj)

  def prjR[T[_[_]]: BirecursiveT, F[_]: Traverse, G[_]: Traverse](orig: T[G])(implicit F: F :<: G):
      Option[T[F]] =
    orig.transAnaM[Option, T[F], F](F.prj)

  implicit final class ListOps[A](val self: List[A]) extends scala.AnyVal {
    final def mapAccumM[B, C, M[_]: Monad](c: C)(f: (C, A) => M[(C, B)]): M[(C, List[B])] =
      self.foldLeftM((c, List.empty[B])){ case ((c, resultList), a) =>
        f(c, a).map { case (newC, b) =>
          (newC, b :: resultList)
        }
      }
    final def mapAccumLeftM[B, C, M[_]: Monad](c: C)(f: (C, A) => M[(C, B)]): M[(C, List[B])] =
      mapAccumM(c)(f).map { case (c, result) => (c, result.reverse) }
  }

  implicit def coproductEqual[F[_], G[_]](implicit F: Delay[Equal, F], G: Delay[Equal, G]): Delay[Equal, Coproduct[F, G, ?]] =
    Delay.fromNT(λ[Equal ~> DelayedFG[F, G]#Equal](eq =>
      Equal equal ((cp1, cp2) =>
        (cp1.run, cp2.run) match {
          case (-\/(f1), -\/(f2)) => F(eq).equal(f1, f2)
          case (\/-(g1), \/-(g2)) => G(eq).equal(g1, g2)
          case (_,       _)       => false
        })))

  type ACopK[a] = CopK[_, a]
  type :<<:[F[_], G[a] <: ACopK[a]] = CopK.Inject[F, G]

  implicit def coproductShow[F[_], G[_]](implicit F: Delay[Show, F], G: Delay[Show, G]): Delay[Show, Coproduct[F, G, ?]] =
    Delay.fromNT(λ[Show ~> DelayedFG[F, G]#Show](sh =>
      Show show (_.run.fold(F(sh).show, G(sh).show))))

  implicit def constEqual[A: Equal]: Delay[Equal, Const[A, ?]] =
    Delay.fromNT(λ[Equal ~> DelayedA[A]#Equal](_ =>
      Equal equal (_.getConst ≟ _.getConst)))

  implicit def constShow[A: Show]: Delay[Show, Const[A, ?]] =
    Delay.fromNT(λ[Show ~> DelayedA[A]#Show](_ =>
      Show show (Show[A] show _.getConst)))

  implicit def sizedEqual[A: Equal, N <: Nat]: Equal[Sized[A, N]] =
    Equal.equal((a, b) => a.unsized ≟ b.unsized)

  implicit def sizedShow[A: Show, N <: Nat]: Show[Sized[A, N]] =
    Show.showFromToString

  implicit def natEqual[N <: Nat]: Equal[N] = Equal.equal((a, b) => true)
  implicit def natShow[N <: Nat]: Show[N] = Show.showFromToString

  implicit def finEqual[N <: Succ[_]]: Equal[Fin[N]] = Equal.equal((a, b) => true)
  implicit def finShow[N <: Succ[_]]: Show[Fin[N]] = Show.showFromToString

  implicit val symbolShow: Show[Symbol] = Show.showFromToString
  implicit val symbolOrder: Order[Symbol] = Order.orderBy(_.name)

  implicit final class LensOps[S, T, A, B](val plens: PLens[S, T, A, B]) extends scala.AnyVal {
    def store(s: S): IndexedStore[A, B, T] =
      IndexedStoreT.indexedStore(plens.get(s))((b: B) => plens.set(b)(s))
  }

  implicit final class QuasarFreeOps[F[_], A](val self: Free[F, A]) extends scala.AnyVal {
    type Self    = Free[F, A]
    type Step[X] = F[X] \/ A

    def resumeTwice(implicit F: Functor[F]): Step[Step[Self]] =
      self.resume leftMap (_ map (_.resume))
  }

  def liftCoM[T[_[_]], M[_]: Applicative, F[_], A, B](f: F[B] => M[CoEnv[A, F, B]])
      : CoEnv[A, F, B] => M[CoEnv[A, F, B]] =
    co => co.run.fold(κ(co.point[M]), f)

  def liftCo[T[_[_]], F[_], A, B](f: F[B] => CoEnv[A, F, B])
      : CoEnv[A, F, B] => CoEnv[A, F, B] =
    liftCoM[T, Id, F, A, B](f)

  def idPrism[F[_]] = PrismNT.id[F]

  def coenvPrism[F[_], A] = PrismNT.coEnv[F, A]

  def coenvBijection[T[_[_]]: BirecursiveT, F[_]: Functor, A]:
      Bijection[Free[F, A], T[CoEnv[A, F, ?]]] =
    bijection[Id, Id, Free[F, A], T[CoEnv[A, F, ?]]](
      _.convertTo[T[CoEnv[A, F, ?]]],
      _.convertTo[Free[F, A]])

  def applyFrom[A, B](bij: Bijection[A, B])(modify: B => B): A => A =
    bij.toK >>> kleisli[Id, B, B](modify) >>> bij.fromK

  def applyCoEnvFrom[T[_[_]]: BirecursiveT, F[_]: Functor, A](
    modify: T[CoEnv[A, F, ?]] => T[CoEnv[A, F, ?]]):
      Free[F, A] => Free[F, A] =
    applyFrom[Free[F, A], T[CoEnv[A, F, ?]]](coenvBijection[T, F, A])(modify)

}

package fp {
  /** Lift a `State` computation to operate over a "larger" state given a `Lens`.
    *
    * NB: Uses partial application of `F[_]` for better type inference, usage:
    *
    *   `zoomNT[F](lens)`
    */
  object zoomNT {
    def apply[F[_]]: Aux[F] =
      new Aux[F]

    final class Aux[F[_]] {
      type ST[S, A] = StateT[F, S, A]
      def apply[A, B](lens: Lens[A, B])(implicit M: Monad[F]): ST[B, ?] ~> ST[A, ?] =
        new (ST[B, ?] ~> ST[A, ?]) {
          def apply[C](s: ST[B, C]) =
            StateT((a: A) => s.run(lens.get(a)).map(_.leftMap(lens.set(_)(a))))
        }
    }
  }

  // type Delay[F[_], G[_]] = F ~> λ[A => F[G[A]]]
  trait DelayedA[A] {
    /** The B is discarded in each case; the type was fixed by A. */
    type Show[B]       = scalaz.Show[Const[A, B]]
    type Equal[B]      = scalaz.Equal[Const[A, B]]
    type RenderTree[B] = quasar.RenderTree[Const[A, B]]
  }
  trait DelayedFG[F[_], G[_]] {
    type Equal[A]      = scalaz.Equal[Coproduct[F, G, A]]
    type Show[A]       = scalaz.Show[Coproduct[F, G, A]]
    type RenderTree[A] = quasar.RenderTree[Coproduct[F, G, A]]
  }
}

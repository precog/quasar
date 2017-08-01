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

package quasar.sst

import slamdata.Predef._
import quasar.contrib.matryoshka._
import quasar.tpe._

import matryoshka._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._
import simulacrum._

/** Typeclass defining how to structurally merge a type, where possible. */
@typeclass
trait StructuralMerge[F[_]] {
  type P[A, B] = EnvT[A, F, B]

  def merge[V, T](pp: (P[V, T], P[V, T]))(implicit V: Semigroup[V], T: Corecursive.Aux[T, P[V, ?]])
    : Option[P[V, T \/ (T, T)]]
}

object StructuralMerge {
  trait PF[F[_]] extends StructuralMerge[F] {
    def mergePF[V, T](implicit V: Semigroup[V], T: Corecursive.Aux[T, P[V, ?]])
      : PartialFunction[(P[V, T], P[V, T]),P[V, T \/ (T, T)]]

    def merge[V, T](pp: (P[V, T], P[V, T]))(implicit V: Semigroup[V], T: Corecursive.Aux[T, P[V, ?]]) =
      mergePF[V, T].lift(pp)
  }

  implicit def coproductStructuralMerge[F[_]: Functor, G[_]: Functor](
    implicit
    F: StructuralMerge[F],
    G: StructuralMerge[G]
  ): StructuralMerge[Coproduct[F, G, ?]] =
    new StructuralMerge[Coproduct[F, G, ?]] {
      type CP[A] = Coproduct[F, G, A]

      def merge[V, T](pp: (P[V, T], P[V, T]))(implicit V: Semigroup[V], T: Corecursive.Aux[T, P[V, ?]]) = {
        implicit val FC: Corecursive.Aux[T, EnvT[V, F, ?]] = derivedEnvTCorec[T, V, CP, F]
        implicit val GC: Corecursive.Aux[T, EnvT[V, G, ?]] = derivedEnvTCorec[T, V, CP, G]

        pp match {
          case (EnvT((x, Coproduct(-\/(l)))), EnvT((y, Coproduct(-\/(r))))) =>
            F.merge[V, T]((envT(x, l), envT(y, r)))
              .map(EnvT.hmap(Inject[F, CP])(_))

          case (EnvT((x, Coproduct(\/-(l)))), EnvT((y, Coproduct(\/-(r))))) =>
            G.merge[V, T]((envT(x, l), envT(y, r)))
              .map(EnvT.hmap(Inject[G, CP])(_))

          case _ => none
        }
      }
    }

  implicit val taggedStructuralMerge: StructuralMerge[Tagged] =
    new StructuralMerge.PF[Tagged] {
      def mergePF[V, T](implicit V: Semigroup[V], T: Corecursive.Aux[T, P[V, ?]]) = {
        case (EnvT((x, Tagged(l1, t1))), EnvT((y, Tagged(l2, t2)))) if (l1 ≟ l2) =>
          envT(x |+| y, Tagged(l1, (t1, t2).right))
      }
    }

  implicit def typeFStructuralMerge[L: Order]: StructuralMerge[TypeF[L, ?]] =
    new StructuralMerge.PF[TypeF[L, ?]] {
      import TypeF._

      def mergePF[V, T](implicit V: Semigroup[V], T: Corecursive.Aux[T, P[V, ?]]) = {
        type TT = (T, T)
        type R  = T \/ TT

        def mergeThese[F[_]: Align](xs: F[T], ys: F[T]): F[R] =
          xs.alignWith(ys)(_.fold(_.left, _.left, (_, _).right))

        def mergeArr(l: (V, IList[T] \/ T), r: (V, IList[T] \/ T)): TypeF[L, R] =
          (l, r) match {
            case ((_, -\/(              xs)), (_, -\/(              ys))) => arr[L, R](mergeThese(xs, ys).left)
            case ((_, \/-(               x)), (_, \/-(               y))) => arr[L, R]((x, y).right.right)
            case ((_, -\/(          INil())), (_, y @ \/-(           _))) => arr[L, R](y map (_.left))
            case ((_, x @ \/-(           _)), (_, -\/(          INil()))) => arr[L, R](x map (_.left))
            case ((_, -\/(ICons(x, INil()))), (_, \/-(               y))) => arr[L, R]((x, y).right.right)
            case ((_, \/-(               x)), (_, -\/(ICons(y, INil())))) => arr[L, R]((x, y).right.right)

            case ((v, -\/(ICons(a, ICons(b, cs)))), (_, \/-(y))) =>
              arr[L, R]((envT(v, union[L, T](a, b, cs)).embed, y).right.right)

            case ((_, \/-(x)), (v, -\/(ICons(a, ICons(b, cs))))) =>
              arr[L, R]((x, envT(v, union[L, T](a, b, cs)).embed).right.right)
          }

        def mergeUnk(xu: Option[TT], yu: Option[TT]): Option[(R, R)] =
          (xu, yu) match {
            case (Some((xk, xv)), Some((yk, yv))) => some(((xk, yk).right, (xv, yv).right))
            case (Some((xk, xv)),           None) => some((xk.left, xv.left))
            case (          None, Some((yk, yv))) => some((yk.left, yv.left))
            case (          None,           None) => none
          }

        {
          case (x, EnvT((_, Bottom()))) => x map (_.left)
          case (EnvT((_, Bottom())), y) => y map (_.left)

          case (EnvT((v, Top())), EnvT((w, Top()))) =>
            envT(v |+| w, top[L, R]())

          case (EnvT((v, Simple(x))), EnvT((w, Simple(y)))) if (x ≟ y) =>
            envT(v |+| w, simple[L, R](x))

          case (EnvT((v, Const(x))), EnvT((w, Const(y)))) if (x ≟ y) =>
            envT(v |+| w, const[L, R](x))

          case (EnvT((v, Arr(x))), EnvT((w, Arr(y)))) =>
            envT(v |+| w, mergeArr((v, x), (w, y)))

          case (EnvT((v, Map(xs, xunk))), EnvT((w, Map(ys, yunk)))) =>
            envT(v |+| w, map[L, R](mergeThese(xs, ys), mergeUnk(xunk, yunk)))

          case (EnvT((v, Unioned(xs))), EnvT((w, Unioned(ys)))) =>
            envT(v |+| w, union[L, T](xs.head, ys.head, xs.tail ::: ys.tail) map (_.left))

          case (x @ EnvT((v, _)), EnvT((w, Unioned(ys)))) =>
            envT(v |+| w, union[L, T](x.embed, ys.head, ys.tail) map (_.left))

          case (EnvT((v, Unioned(xs))), y @ EnvT((w, _))) =>
            envT(v |+| w, union[L, T](y.embed, xs.head, xs.tail) map (_.left))
        }
      }
    }

  ////

  private def derivedEnvTCorec[T, A, G[_]: Functor, F[_]](
    implicit
    F: F :<: G,
    GC: Corecursive.Aux[T, EnvT[A, G, ?]]
  ): Corecursive.Aux[T, EnvT[A, F, ?]] =
    new Corecursive[T] {
      type Base[B] = EnvT[A, F, B]

      def embed(ft: Base[T])(implicit BF: Functor[Base]) =
        GC.embed(EnvT.hmap(F)(ft))
    }
}

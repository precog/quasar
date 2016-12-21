/*
 * Copyright 2014–2016 SlamData Inc.
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

package quasar.qscript

import quasar.Predef._
import quasar.{NonTerminal, RenderTree, RenderTreeT}, RenderTree.ops._
import quasar.fp._

import matryoshka._
import matryoshka.data._
import monocle.macros.Lenses
import scalaz._, Scalaz._

/** Projections are technically dimensional (i.e., QScript) operations. However,
  * to a filesystem, they are merely Map operations. So, we use these components
  * while building the QScript plan and they are then used in static path
  * processing, but they are replaced with equivalent MapFuncs before being
  * processed by the filesystem.
  */
sealed abstract class ProjectBucket[T[_[_]], A] {
  def src: A
}

@Lenses final case class BucketField[T[_[_]], A](
  src: A,
  value: FreeMap[T],
  name: FreeMap[T])
    extends ProjectBucket[T, A]

@Lenses final case class BucketIndex[T[_[_]], A](
  src: A,
  value: FreeMap[T],
  index: FreeMap[T])
    extends ProjectBucket[T, A]

object ProjectBucket {
  implicit def equal[T[_[_]]: EqualT]: Delay[Equal, ProjectBucket[T, ?]] =
    new Delay[Equal, ProjectBucket[T, ?]] {
      def apply[A](eq: Equal[A]) =
        Equal.equal {
          case (BucketField(a1, v1, n1), BucketField(a2, v2, n2)) =>
            eq.equal(a1, a2) && v1 ≟ v2 && n1 ≟ n2
          case (BucketIndex(a1, v1, i1), BucketIndex(a2, v2, i2)) =>
            eq.equal(a1, a2) && v1 ≟ v2 && i1 ≟ i2
          case (_, _) => false
        }
    }

  implicit def traverse[T[_[_]]]: Traverse[ProjectBucket[T, ?]] =
    new Traverse[ProjectBucket[T, ?]] {
      def traverseImpl[G[_], A, B](
        fa: ProjectBucket[T, A])(
        f: A => G[B])(
        implicit G: Applicative[G]):
          G[ProjectBucket[T, B]] = fa match {
        case BucketField(src, values, name) =>
          f(src) ∘ (BucketField(_, values, name))
        case BucketIndex(src, values, index) =>
          f(src) ∘ (BucketIndex(_, values, index))
      }
    }

  implicit def show[T[_[_]]: ShowT]: Delay[Show, ProjectBucket[T, ?]] =
    new Delay[Show, ProjectBucket[T, ?]] {
      def apply[A](sh: Show[A]): Show[ProjectBucket[T, A]] =
        Show.show {
          case BucketField(a, v, n) => Cord("BucketField(") ++
            sh.show(a) ++ Cord(",") ++
            v.show ++ Cord(",") ++
            n.show ++ Cord(")")
          case BucketIndex(a, v, i) => Cord("BucketIndex(") ++
            sh.show(a) ++ Cord(",") ++
            v.show ++ Cord(",") ++
            i.show ++ Cord(")")
        }
    }

  implicit def renderTree[T[_[_]]: RenderTreeT: ShowT]: Delay[RenderTree, ProjectBucket[T, ?]] =
    new Delay[RenderTree, ProjectBucket[T, ?]] {
      def apply[A](RA: RenderTree[A]): RenderTree[ProjectBucket[T, A]] = RenderTree.make {
        case BucketField(src, value, name) =>
          NonTerminal(List("BucketField"), None, List(
            RA.render(src),
            value.render,
            name.render))
        case BucketIndex(src, value, index) =>
          NonTerminal(List("BucketIndex"), None, List(
            RA.render(src),
            value.render,
            index.render))
      }
    }

  implicit def mergeable[T[_[_]]: CorecursiveT: EqualT]:
      Mergeable.Aux[T, ProjectBucket[T, ?]] =
    new Mergeable[ProjectBucket[T, ?]] {
      type IT[F[_]] = T[F]

      def mergeSrcs(
        left: FreeMap[IT],
        right: FreeMap[IT],
        p1: ProjectBucket[IT, ExternallyManaged],
        p2: ProjectBucket[IT, ExternallyManaged]) =
        (p1, p2) match {
          case (BucketField(s1, v1, n1), BucketField(s2, v2, n2)) =>
            val new1: ProjectBucket[T, ExternallyManaged] = BucketField(s1, v1 >> left, n1 >> left)
            val new2: ProjectBucket[T, ExternallyManaged] = BucketField(s2, v2 >> right, n2 >> right)
            (new1 ≟ new2).option(SrcMerge(new1, HoleF[IT], HoleF[IT]))
          case (BucketIndex(s1, v1, n1), BucketIndex(s2, v2, n2)) =>
            val new1: ProjectBucket[T, ExternallyManaged] = BucketIndex(s1, v1 >> left, n1 >> left)
            val new2: ProjectBucket[T, ExternallyManaged] = BucketIndex(s2, v2 >> right, n2 >> right)
            (new1 ≟ new2).option(SrcMerge(new1, HoleF[IT], HoleF[IT]))
          case (_, _) => None
      }
    }
}

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
import quasar.ejson.{EJsonArbitrary, TypeTag}
import quasar.pkg.tests.{arbitrary => tarb, _}

import matryoshka.Delay
import scalaz.scalacheck.ScalaCheckBinding._
import scalaz._, Scalaz._

/** An ADT representing the two forms of type tagging that quasar does in EJson.
  *
  * This exists primarily for the arbitrary instance, used by TypedEJson to
  * generate arbitrary EJson that contains the particular `Meta` nodes that
  * represent type tags.
  */
private[sst] sealed abstract class TypeMetadata[A]

object TypeMetadata {
  final case class Type[A](tag: TypeTag, value: A) extends TypeMetadata[A]
  final case class SizedType[A](tag: TypeTag, size: BigInt, value: A) extends TypeMetadata[A]
  // NB: For more control over generation frequency.
  final case class Absent[A](value: A) extends TypeMetadata[A]
  // NB: Used as a termination case in the generator.
  final case class Null[A]() extends TypeMetadata[A]

  implicit val arbitrary: Delay[Arbitrary, TypeMetadata] =
    new PatternArbitrary[TypeMetadata] {
      import EJsonArbitrary._

      def leafGenerators[A] =
        uniformly(const(Null[A]()))

      def branchGenerators[A: Arbitrary] =
        NonEmptyList(
          (700, tarb[A] ^^ Absent[A]),
          (200, (tarb[TypeTag] ⊛ tarb[A])(Type(_, _))),
          (100, (tarb[TypeTag] ⊛ genBigInt ⊛ tarb[A])(SizedType(_, _, _))))
    }

  implicit val traverse: Traverse[TypeMetadata] =
    new Traverse[TypeMetadata] with Foldable.FromFoldr[TypeMetadata] {
      def traverseImpl[G[_]: Applicative, A, B](fa: TypeMetadata[A])(f: A => G[B]): G[TypeMetadata[B]] =
        fa match {
          case Type(t, a)         => f(a) map (Type(t, _))
          case SizedType(t, n, a) => f(a) map (SizedType(t, n, _))
          case Absent(a)          => f(a) map (Absent(_))
          case Null()             => (Null(): TypeMetadata[B]).point[G]
        }
    }
}

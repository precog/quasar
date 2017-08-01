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
import quasar.{ejson => ejs}
import quasar.ejson.{EJson, EncodeEJsonK, ExtEJson => E, TypeTag}
import quasar.ejson.implicits._

import matryoshka._
import matryoshka.implicits._
import scalaz._, Scalaz._

final case class Tagged[A](tag: TypeTag, value: A)

object Tagged {
  implicit val traverse1: Traverse1[Tagged] =
    new Traverse1[Tagged] {
      def foldMapRight1[A, B](fa: Tagged[A])(z: A => B)(f: (A, => B) => B): B =
        z(fa.value)

      def traverse1Impl[G[_]: Apply, A, B](fa: Tagged[A])(f: A => G[B]): G[Tagged[B]] =
        f(fa.value) map (b => fa.copy(value = b))
    }

  implicit val encodeEJsonK: EncodeEJsonK[Tagged] =
    new EncodeEJsonK[Tagged] {
      def encodeK[J](implicit JC: Corecursive.Aux[J, EJson], JR: Recursive.Aux[J, EJson]): Algebra[Tagged, J] = {
        case Tagged(t, j) => E(ejs.Meta(j, ejs.Type(t))).embed
      }
    }

  implicit val equal: Delay[Equal, Tagged] =
    new Delay[Equal, Tagged] {
      def apply[A](eql: Equal[A]) = {
        implicit val eqlA: Equal[A] = eql
        Equal.equalBy(t => (t.tag, t.value))
      }
    }

  implicit val show: Delay[Show, Tagged] =
    new Delay[Show, Tagged] {
      def apply[A](shw: Show[A]) = {
        implicit val shwA: Show[A] = shw
        Show.shows(t => s"${t.value.shows} @ ${t.tag.value}")
      }
    }
}

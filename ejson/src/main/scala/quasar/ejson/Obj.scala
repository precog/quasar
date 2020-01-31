/*
 * Copyright 2014–2020 SlamData Inc.
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

package quasar.ejson

import slamdata.Predef.{Map => SMap, _}
import quasar.{RenderTree, NonTerminal}, RenderTree.ops._
import quasar.fp._

import matryoshka._
import monocle.Iso
import scalaz.{Applicative, Equal, Order, Scalaz, Show, Traverse}, Scalaz._

final case class Obj[A](value: ListMap[String, A]) {
  def obj[A] =
    Iso[Obj[A], ListMap[String, A]](_.value)(Obj(_))
}

object Obj extends ObjInstances

sealed abstract class ObjInstances extends ObjInstances0 {
  implicit val traverse: Traverse[Obj] = new Traverse[Obj] {
    def traverseImpl[G[_]: Applicative, A, B](fa: Obj[A])(f: A => G[B]):
        G[Obj[B]] =
      fa.value.traverse(f).map(Obj(_))
  }

  implicit val order: Delay[Order, Obj] =
    new Delay[Order, Obj] {
      def apply[α](ord: Order[α]) = {
        implicit val ordA: Order[α] = ord
        Order.orderBy(_.value: SMap[String, α])
      }
    }

  implicit val show: Delay[Show, Obj] =
    new Delay[Show, Obj] {
      def apply[α](shw: Show[α]) = {
        implicit val shwA: Show[α] = shw
        Show.show(o => (o.value: SMap[String, α]).show)
      }
    }

  implicit val renderTree: Delay[RenderTree, Obj] =
    new Delay[RenderTree, Obj] {
      def apply[A](rt: RenderTree[A]) = {
        implicit val rtA = rt
        RenderTree.make(obj => NonTerminal(List("Obj"), None, List(obj.value.render)))
      }
    }
}

sealed abstract class ObjInstances0 {
  implicit val equal: Delay[Equal, Obj] =
    new Delay[Equal, Obj] {
      def apply[α](eql: Equal[α]) = {
        implicit val eqlA: Equal[α] = eql
        Equal.equalBy(_.value: SMap[String, α])
      }
    }
}

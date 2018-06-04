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

package quasar.qsu

import slamdata.Predef.Symbol
import quasar.ejson.implicits._
import quasar.fp._
import quasar.contrib.iota._
import quasar.contrib.matryoshka._
import quasar.qscript.{FreeMap, FreeMapA}

import matryoshka.{delayShow, delayEqual, equalTEqual, showTShow, BirecursiveT, ShowT, EqualT}
import matryoshka.data._
// import matryoshka.data.free._
import scalaz.{\/, Cord, Equal, IMap, ISet, Monoid, Order, Show}
import scalaz.std.list._
import scalaz.std.option._
import scalaz.std.tuple._
import scalaz.syntax.applicative._
import scalaz.syntax.semigroup._
import scalaz.syntax.std.option._
import matryoshka.{Hole => _, _}
import matryoshka.data._

final case class References[T[_[_]], D](
    accessing: References.Accessing[T, D],
    accessed: References.Accessed[D]) {

  def modifyAccess(of: Access[D, Symbol])(f: FreeMap[T] => FreeMap[T])(implicit D: Order[D]): References[T, D] =
    modifySomeAccess(of, ISet.empty)(f)

  def modifySomeAccess(of: Access[D, Symbol], except: ISet[Symbol])(f: FreeMap[T] => FreeMap[T])(implicit D: Order[D]): References[T, D] =
    accessed.lookup(of).fold(this) { syms =>
      copy(accessing = (syms \\ except).foldLeft(accessing)(_.adjust(_, _.adjust(of, f))))
    }

  def recordAccess(by: Symbol, of: Access[D, Symbol], as: FreeMap[T])(implicit D: Order[D]): References[T, D] = {
    val accesses = IMap.singleton(of, as)
    val accessing1 = accessing.alter(by, _ map (_ union accesses) orElse some(accesses))
    val accessed1 = accessed |+| IMap.singleton(of, ISet singleton by)
    References(accessing1, accessed1)
  }

  // Replace all accesses by `prev` with `next`.
  def replaceAccess(prev: Symbol, next: Symbol)(implicit D: Order[D]): References[T, D] =
    accessing.lookup(prev).fold(this) { accesses =>
      val nextAccessed = accesses.keySet.foldLeft(accessed) { (m, a) =>
        m.adjust(a, _.delete(prev).insert(next))
      }

      References(accessing.delete(prev).insert(next, accesses), nextAccessed)
    }

  // Resolves all `Access` made by `by` in the given `FreeAccess`, resulting in
  // a `FreeMap`.
  def resolveAccess[A, B]
    (by: Symbol, in: FreeMapA[T, A])
    (f: B => Symbol)
    (ex: A => Access[D, B] \/ B)
    (implicit D: Order[D])
    : FreeMapA[T, B] =
      accessing.lookup(by).fold(in.map(a => ex(a).fold(Access.src.get(_), b => b))) { accesses =>
        in flatMap { a =>
          ex(a).fold({ access =>
            val b = Access.src.get(access)
            accesses.lookup(access.symbolic(f)).cata(_.as(b), b.point[FreeMapA[T, ?]])
          }, {
            _.point[FreeMapA[T, ?]]
          })
        }
      }
}

object References {
  // How to access a given identity or value.
  type Accesses[T[_[_]], D] = IMap[Access[D, Symbol], FreeMap[T]]
  // Accesses by vertex.
  type Accessing[T[_[_]], D] = IMap[Symbol, Accesses[T, D]]
  // Vertices by access.
  type Accessed[D] = IMap[Access[D, Symbol], ISet[Symbol]]

  def noRefs[T[_[_]], D]: References[T, D] =
    References(IMap.empty, IMap.empty)

  implicit def monoid[T[_[_]], D: Order]: Monoid[References[T, D]] =
    Monoid.instance(
      (x, y) => References(
        x.accessing.unionWith(y.accessing)(_ union _),
        x.accessed |+| y.accessed),
      noRefs)

  implicit def equal[T[_[_]]: BirecursiveT: EqualT, D: Equal]: Equal[References[T, D]] = {
    Equal.equalBy(r => (r.accessing, r.accessed))
  }

  implicit def show[T[_[_]]: ShowT, D: Show]: Show[References[T, D]] =
    Show.show {
      case References(accessing, accessed) =>
        Cord("References {\n\n") ++
        Cord("Accessing[\n") ++
        Cord(printMultiline(accessing.toList)) ++
        Cord("]\n\nAccessed[\n") ++
        Cord(printMultiline(accessed.toList)) ++
        Cord("]\n}")
    }
}

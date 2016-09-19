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

package quasar.fp

import quasar.Predef._

import matryoshka._
import scalaz._

/** "Generic" types for building partially-constructed trees in some
  * "functorized" type. */
// TODO: submit to matryoshka?
package object tree {
  /** A tree structure with one kind of hole. See `eval1`. */
  type Unary[F[_]] = Free[F, UnaryArg]
  object Unary {
    def arg[F[_]]: Unary[F] = Free.pure(UnaryArg._1)
  }

  /** A tree structure with two kinds of holes. See `eval2`. */
  type Binary[F[_]] = Free[F, BinaryArg]
  object Binary {
    def arg1[F[_]]: Binary[F] = Free.pure(BinaryArg._1)
    def arg2[F[_]]: Binary[F] = Free.pure(BinaryArg._2)
  }

  /** A tree structure with three kinds of holes. See `eval3`. */
  type Ternary[F[_]] = Free[F, TernaryArg]
  object Ternary {
    def arg1[F[_]]: Ternary[F] = Free.pure(TernaryArg._1)
    def arg2[F[_]]: Ternary[F] = Free.pure(TernaryArg._2)
    def arg3[F[_]]: Ternary[F] = Free.pure(TernaryArg._3)
  }

  trait UnaryArg {
    def fold[A](arg1: A): A = arg1
  }
  object UnaryArg {
    case object _1 extends UnaryArg
  }
  trait BinaryArg {
    def fold[A](arg1: A, arg2: A): A = this match {
      case BinaryArg._1 => arg1
      case BinaryArg._2 => arg2
    }
  }
  object BinaryArg {
    case object _1 extends BinaryArg
    case object _2 extends BinaryArg
  }
  trait TernaryArg {
    def fold[A](arg1: A, arg2: A, arg3: A): A = this match {
      case TernaryArg._1 => arg1
      case TernaryArg._2 => arg2
      case TernaryArg._3 => arg3
    }
  }
  object TernaryArg {
    case object _1 extends TernaryArg
    case object _2 extends TernaryArg
    case object _3 extends TernaryArg
  }

  implicit class UnaryOps[F[_]](self: Unary[F]) {
    def eval[T[_[_]]: Corecursive](implicit F: Traverse[F]): T[F] => T[F] =
      arg => freeCata[F, UnaryArg, T[F]](self)(interpret(_.fold(arg), _.embed))
  }

  implicit class BinaryOps[F[_]](self: Binary[F]) {
    def eval[T[_[_]]: Corecursive](implicit F: Traverse[F]): (T[F], T[F]) => T[F] =
      (arg1, arg2) => freeCata[F, BinaryArg, T[F]](self)(interpret(_.fold(arg1, arg2), _.embed))
  }

  implicit class TernaryOps[F[_]](self: Ternary[F]) {
    def eval[T[_[_]]: Corecursive](implicit F: Traverse[F]): (T[F], T[F], T[F]) => T[F] =
      (arg1, arg2, arg3) => freeCata[F, TernaryArg, T[F]](self)(interpret(_.fold(arg1, arg2, arg3), _.embed))
  }
}

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

package quasar.physical.marklogic.qscript

import slamdata.Predef.{Eq =>_, _}
import quasar.fp._
import quasar.qscript.MapFuncsCore._
import quasar.qscript._
import matryoshka._
import matryoshka.implicits._
import matryoshka.data._

import scalaz._, Scalaz._

final class RewriteNullSpec extends quasar.Qspec {
  def eq[T[_[_]]: BirecursiveT, A](lhs: FreeMapA[T, A], rhs: FreeMapA[T, A]): FreeMapA[T, A] =
    Free.roll(MFC(Eq(lhs, rhs)))

  def neq[T[_[_]]: BirecursiveT, A](lhs: FreeMapA[T, A], rhs: FreeMapA[T, A]): FreeMapA[T, A] =
    Free.roll(MFC(Neq(lhs, rhs)))

  def typeOf[T[_[_]]: BirecursiveT, A](fm: FreeMapA[T, A]): FreeMapA[T, A] =
    Free.roll(MFC(TypeOf(fm)))

  def rewrite(fm: FreeMapA[Fix, Unit]): FreeMapA[Fix, Unit] =
    fm.transCata[FreeMapA[Fix, Unit]](rewriteNullCheck[Fix, FreeMapA[Fix, Unit], Unit])

  val expr = StrLit[Fix, Unit]("foo")
  val nullStr = StrLit[Fix, Unit]("null")

  "rewriteNullCheck" should {
    "rewrite Eq(Null, rhs) into Eq(TypeOf(rhs), 'null')" in {
      rewrite(eq(NullLit(), expr)) must equal(eq(typeOf(expr), nullStr))
    }

    "rewrite Eq(lhs, Null) into Eq(TypeOf(lhs), 'null')" in {
      rewrite(eq(expr, NullLit())) must equal(eq(typeOf(expr), nullStr))
    }

    "rewrite Neq(Null, rhs) into Neq(TypeOf(rhs), 'null')" in {
      rewrite(neq(NullLit(), expr)) must equal(neq(typeOf(expr), nullStr))
    }

    "rewrite Neq(lhs, Null) into Neq(TypeOf(lhs), 'null')" in {
      rewrite(neq(expr, NullLit())) must equal(neq(typeOf(expr), nullStr))
    }
  }
}

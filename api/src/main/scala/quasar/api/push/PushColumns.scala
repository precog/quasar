/*
 * Copyright 2020 Precog Data
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

package quasar.api.push

import slamdata.Predef.{Eq => _, _}

import cats._
import cats.data.NonEmptyList
import cats.implicits._

import PushColumns._

/**
 * Data structure represented either non-empty list w/o special entry
 * or non-empty list with special entry
 */
sealed trait PushColumns[A] extends Product with Serializable { self =>
  def toNel: NonEmptyList[A] = self match {
    case NoPrimary(nel) => nel
    case HasPrimary(left, hd, right) => left match {
      case List() => NonEmptyList(hd, right)
      case a :: ltail => NonEmptyList(a, ltail) concatNel NonEmptyList(hd, right)
    }
  }
  def toList: List[A] = toNel.toList

  def primary: Option[A] = self match {
    case NoPrimary(_) => none[A]
    case HasPrimary(_, head, _) => head.some
  }
}

object PushColumns {
  final case class NoPrimary[A](value: NonEmptyList[A]) extends PushColumns[A]
  final case class HasPrimary[A](left: List[A], head: A, right: List[A]) extends PushColumns[A]

  implicit def traversePushColumns[A]: Traverse[PushColumns] = new Traverse[PushColumns] {
    override def map[A, B](fa: PushColumns[A])(f: A => B): PushColumns[B] = fa match {
      case NoPrimary(nel) => NoPrimary(nel.map(f))
      case HasPrimary(left, head, right) => HasPrimary(left.map(f), f(head), right.map(f))
    }
    def traverse[G[_]: Applicative, A, B](fa: PushColumns[A])(f: A => G[B]): G[PushColumns[B]] = fa match {
      case NoPrimary(nel) =>
        nel.traverse(f).map(NoPrimary(_))
      case HasPrimary(left, head, right) =>
        Apply[G].map3(left.traverse(f), f(head), right.traverse(f))(HasPrimary(_, _, _))
    }

    def foldLeft[A, B](fa: PushColumns[A], b: B)(f: (B, A) => B): B = fa match {
      case NoPrimary(nel) => nel.foldLeft(b)(f)
      case HasPrimary(left, head, right) =>
        val lefted = left.foldLeft(b)(f)
        val centered = f(lefted, head)
        right.foldLeft(centered)(f)
    }

    def foldRight[A, B](fa: PushColumns[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] = fa match {
      case NoPrimary(nel) => nel.foldRight(lb)(f)
      case HasPrimary(left, head, right) =>
        val righted = right.foldRight(lb)(f)
        val centered = f(head, righted)
        left.foldRight(centered)(f)
    }
  }
  implicit def eqPushColumns[A: Eq]: Eq[PushColumns[A]] = Eq.by {
    case NoPrimary(nel) => (Some(nel), None)
    case HasPrimary(left, head, right) => (None, Some((left, head, right)))
  }
  implicit def showPushColumns[A: Show]: Show[PushColumns[A]] = Show.show {
    case NoPrimary(nel) => s"NoPrimary(${nel.show})"
    case HasPrimary(left, head, right) => s"HasPrimary(${left.show}, ${head.show}, ${right.show})"
  }
}


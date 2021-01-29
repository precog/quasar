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

package quasar

import slamdata.Predef.{Eq => _, _}

import cats._
import cats.data.NonEmptyList
import cats.implicits._

import PointedList._

/**
 * Data structure represented either non-empty list w/o special entry
 * or non-empty list with special entry
 */
sealed trait PointedList[+A] extends Product with Serializable { self =>
  def toNel: NonEmptyList[A] = self match {
    case NotPointed(nel) => nel
    case Pointed(hd, tail) => NonEmptyList(hd, tail)
  }
  def toList: List[A] = toNel.toList

  def point[AA >: A]: Option[AA] = self match {
    case NotPointed(_) => none[AA]
    case Pointed(head, _) => head.some
  }
}

object PointedList {
  final case class NotPointed[+A](value: NonEmptyList[A]) extends PointedList[A]
  final case class Pointed[+A](head: A, tail: List[A]) extends PointedList[A]

  implicit def traversePointedList[A]: Traverse[PointedList] = new Traverse[PointedList] {
    override def map[A, B](fa: PointedList[A])(f: A => B): PointedList[B] = fa match {
      case NotPointed(nel) => NotPointed(nel.map(f))
      case Pointed(head, tail) => Pointed(f(head), tail.map(f))
    }
    def traverse[G[_]: Applicative, A, B](fa: PointedList[A])(f: A => G[B]): G[PointedList[B]] = fa match {
      case NotPointed(nel) => nel.traverse(f).map(NotPointed(_))
      case Pointed(head, tail) => Apply[G].map2(f(head), tail.traverse(f))(Pointed(_, _))
    }

    def foldLeft[A, B](fa: PointedList[A], b: B)(f: (B, A) => B): B = fa match {
      case NotPointed(nel) => nel.foldLeft(b)(f)
      case Pointed(head, tail) => tail.foldLeft(f(b, head))(f)
    }

    def foldRight[A, B](fa: PointedList[A], lb: Eval[B])(f: (A, Eval[B]) => Eval[B]): Eval[B] = fa match {
      case NotPointed(nel) => nel.foldRight(lb)(f)
      case Pointed(head, tail) => f(head, tail.foldRight(lb)(f))
    }

  }
  implicit def eqPointedList[A: Eq]: Eq[PointedList[A]] = Eq.by {
    case NotPointed(nel) => (Some(nel), None)
    case Pointed(head, tail) => (None, Some((head, tail)))
  }
  implicit def showPointedList[A: Show]: Show[PointedList[A]] = Show.show {
    case NotPointed(nel) => s"NotPointed(${nel.show})"
    case Pointed(head, tail) => s"Pointed(${head.show}, ${tail.show})"
  }
}

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

import cats.Id
import cats.data.{Const, Ior, NonEmptyMap}
import cats.instances.string._

import java.lang.String
import scala._
import scala.Predef._

import skolems.∃

package object param {
  type Formal[A] = ParamType[Id, A]
  type Actual[A] = ParamType[Const[A, ?], A]

  object Formal {
    val boolean: Formal[Boolean] =
      ParamType.Boolean[Id](())

    def integer(bounds: Option[Int Ior Int], step: Option[IntegerStep]): Formal[Int] =
      ParamType.Integer[Id](ParamType.Integer.Args(bounds, step))

    def enum[A](x: (String, A), xs: (String, A)*): Formal[A] =
      ParamType.Enum[Id, A](NonEmptyMap.of(x, xs: _*))

    final case class Pair[A](fst: ∃[Formal], snd: ∃[Formal]) extends Formal[A] {
      val toList: List[∃[Formal]] = toList0(List(), fst, snd).reverse

      @annotation.tailrec
      private def toList0(acc: List[∃[Formal]], fst: ∃[Formal], snd: ∃[Formal]): List[∃[Formal]] = snd match {
        case ∃(Pair(hd, tl)) => toList0(fst :: acc, hd, tl)
        case other => other :: fst :: acc
      }
    }
    object Pair {
      def fromList[A]: List[∃[Formal]] =>  Option[Pair[A]] = {
        case hd :: tl => tl.headOption.map(Pair(hd, _))
        case Nil => None
      }
    }
  }

  object Actual {
    def boolean(b: Boolean): Actual[Boolean] =
      ParamType.Boolean(Const(b))

    def integer(i: Int): Actual[Int] =
      ParamType.Integer(Const(i))

    def enumSelect(s: String): Actual[String] =
      ParamType.EnumSelect(Const(s))

    final case class Pair[A](fst: ∃[Actual], snd: ∃[Actual]) extends Actual[A] {
      val toList: List[∃[Actual]] = toList0(List(), fst, snd).reverse

      @annotation.tailrec
      private def toList0(acc: List[∃[Actual]], fst: ∃[Actual], snd: ∃[Actual]): List[∃[Actual]] = snd match {
        case ∃(Pair(hd, tl)) => toList0(fst :: acc, hd, tl)
        case other => other :: fst :: acc
      }
    }
    object Pair {
      def fromList[A]: List[∃[Actual]] => Option[Pair[A]] = {
        case hd :: tl => tl.headOption.map(Pair(hd, _))
        case Nil => None
      }
    }
  }
}

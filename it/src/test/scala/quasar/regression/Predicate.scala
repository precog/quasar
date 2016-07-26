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

package quasar.regression

import quasar.Predef._

import argonaut._, Argonaut._
import org.specs2.execute._
import org.specs2.matcher._
import scalaz.{Failure => _, _}, Scalaz._
import scalaz.stream._

sealed trait Predicate {
  def apply[F[_]: Catchable: Monad](
    expected: Vector[Json],
    actual: Process[F, Json]
  ): F[Result]
}

object Predicate {
  import MustMatchers._
  import StandardResults._
  import DecodeResult.{ok => jok, fail => jfail}

  def matchJson(expected: Option[Json]): Matcher[Option[Json]] = new Matcher[Option[Json]] {
    def apply[S <: Option[Json]](s: Expectable[S]) = {
      (expected, s.value) match {
        case (Some(expected), Some(actual)) =>
          (actual.obj |@| expected.obj) { (actual, expected) =>
            if (actual.toList == expected.toList)
              success(s"matches $expected", s)
            else if (actual == expected)
              failure(s"$actual matches $expected, but order differs", s)
            else failure(s"$actual does not match $expected", s)
          } getOrElse result(actual == expected, s"matches $expected", s"$actual does not match $expected", s)
        case (Some(v), None)  => failure(s"ran out before expected; missing: ${v}", s)
        case (None, Some(v))  => failure(s"had more than expected: ${v}", s)
        case (None, None)     => success(s"matches (empty)", s)
        case _                => failure(s"scalac is weird", s)
      }
    }
  }

  /** Must contain ALL the elements in any order. */
  final case object ContainsAtLeast extends Predicate {
    def apply[F[_]: Catchable: Monad](
      expected: Vector[Json],
      actual: Process[F, Json]
    ): F[Result] =
      actual.scan((expected.toSet, Set.empty[Json])) {
        case ((expected, wrongOrder), e) =>
          expected.find(_ == e) match {
            case Some(e1) if jsonMatches(e1, e) =>
              (expected.filterNot(_ == e), wrongOrder)
            case Some(_) =>
              (expected.filterNot(_ == e), wrongOrder + e)
            case None =>
              (expected, wrongOrder)
          }
      }
        .runLast
        .map {
          case Some((exp, wrongOrder)) =>
            (exp aka "unmatched expected values" must beEmpty) and
            (wrongOrder aka "matched but field order differs" must beEmpty): Result
          case None =>
            failure
        }
  }

  /** Must contain ALL and ONLY the elements in any order. */
  final case object ContainsExactly extends Predicate {
    def apply[F[_]: Catchable: Monad](
      expected: Vector[Json],
      actual: Process[F, Json]
    ): F[Result] =
      actual.scan((expected.toSet, Set.empty[Json], None: Option[Json])) {
        case ((expected, wrongOrder, extra), e) =>
          expected.find(_ == e) match {
            case Some(e1) if jsonMatches(e1, e) =>
              (expected.filterNot(_ == e), wrongOrder, extra)
            case Some(_) =>
              (expected.filterNot(_ == e), wrongOrder + e, extra)
            case None =>
              (expected, wrongOrder, extra.orElse(e.some))
          }
      }
        .runLast
        .map {
          case Some((exp, wrongOrder, extra)) =>
            (extra aka "unexpected value" must beNone) and
            (wrongOrder aka "matched but field order differs" must beEmpty) and
            (exp aka "unmatched expected values" must beEmpty): Result
          case None =>
            failure
        }
  }

  /** Must EXACTLY match the elements, in order. */
  final case object EqualsExactly extends Predicate {
    def apply[F[_]: Catchable: Monad](
      expected0: Vector[Json],
      actual0: Process[F, Json]
    ): F[Result] = {
      val actual   = actual0.map(Some(_))
      val expected = Process.emitAll(expected0).map(Some(_))

      (actual tee expected)(tee.zipAll(None, None))
        .flatMap { case ((a, e)) =>
          if (jsonMatches(a, e))
            Process.halt
          else
            Process.emit(a must matchJson(e) : Result)
        }
        .runLog.map(_.foldMap()(Result.ResultMonoid))
    }
  }

  /** Must START WITH the elements, in order. */
  final case object EqualsInitial extends Predicate {
    def apply[F[_]: Catchable: Monad](
      expected0: Vector[Json],
      actual0: Process[F, Json]
    ): F[Result] = {
      val actual   = actual0.map(Some(_))
      val expected = Process.emitAll(expected0).map(Some(_))

      (actual tee expected)(tee.zipAll(None, None))
        .flatMap {
          case (a, None) => Process.halt
          case (a, e) if (jsonMatches(a, e)) => Process.halt
          case (a, e) => Process.emit(a must matchJson(e) : Result)
        }
        .runLog.map(_.foldMap()(Result.ResultMonoid))
    }
  }

  /** Must NOT contain ANY of the elements. */
  final case object DoesNotContain extends Predicate {
    def apply[F[_]: Catchable: Monad](
      expected0: Vector[Json],
      actual: Process[F, Json]
    ): F[Result] = {
      val expected = expected0.toSet

      if (expected.isEmpty)
        actual.drain.run.as(failure)
      else
        actual.scan(expected) { case (exp, e) =>
          // NB: want to ignore field-order here
          exp.filterNot(_ == e)
        }
        .dropWhile(_.size == expected.size)
        .take(1)
        .map(unseen => expected.filterNot(unseen contains _) aka "prohibited values" must beEmpty : Result)
        .runLastOr(success)
    }
  }

  private def jsonMatches(j1: Json, j2: Json): Boolean =
    (j1.obj.map(_.toList) |@| j2.obj.map(_.toList))(_ == _) getOrElse (j1 == j2)

  private def jsonMatches(j1: Option[Json], j2: Option[Json]): Boolean =
    (j1 |@| j2)(jsonMatches) getOrElse false

  implicit val PredicateDecodeJson: DecodeJson[Predicate] =
    DecodeJson(c => c.as[String].flatMap {
      case "containsAtLeast"  => jok(ContainsAtLeast)
      case "containsExactly"  => jok(ContainsExactly)
      case "doesNotContain"   => jok(DoesNotContain)
      case "equalsExactly"    => jok(EqualsExactly)
      case "equalsInitial"    => jok(EqualsInitial)
      case str                => jfail("Expected one of: containsAtLeast, containsExactly, doesNotContain, equalsExactly, equalsInitial, but found: " + str, c.history)
    })
}

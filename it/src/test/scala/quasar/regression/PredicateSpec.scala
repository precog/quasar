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

package quasar.regression

import slamdata.Predef.{Stream => _, _}

import quasar.common.data.Data
import quasar.common.data.DataGenerators.dataArbitrary
import quasar.fp._
import quasar.frontend.data.DataCodec

import scala.Predef.$conforms

import argonaut._, Argonaut._
import cats.effect.IO
import fs2.Stream
import org.scalacheck.Prop
import org.specs2.execute._
import org.specs2.matcher._
import org.scalacheck.Arbitrary, Arbitrary._
import scalaz.{Failure => _, Success => _, _}

class PredicateSpec extends quasar.Qspec {
  import Predicate._

  implicit val jsonArbitrary: Arbitrary[Json] = Arbitrary {
    arbitrary[Data].map(DataCodec.Precise.encode(_).getOrElse(jString("Undefined")))
  }

  // NB: for debugging, prints nicely and preserves field order
  implicit val jsonShow: Show[Json] = new Show[Json] {
    override def show(json: Json) = json.pretty(minspace)
  }

  def shuffle[CC[X] <: scala.collection.TraversableOnce[X], A](as: CC[A])
      (implicit bf: scala.collection.generic.CanBuildFrom[CC[A], A, CC[A]]): IO[CC[A]] =
    IO(scala.util.Random.shuffle(as))

  def partial(results: List[Json]): Stream[IO, Json] =
    Stream.emits(results) ++ Stream.eval(IO.raiseError(new RuntimeException()))

  def run(p: Predicate, expected: List[Json], result: List[Json], fieldOrder: OrderSignificance, resultOrder: OrderSignificance): Result =
    p(expected, Stream.emits(result): Stream[IO, Json], fieldOrder, resultOrder).unsafeRunSync

  def shuffledFields(
    json: List[Json], pred: Predicate, fieldOrder: OrderSignificance, resultOrder: OrderSignificance, matcher: Matcher[Result]
  ): Prop = {
    val pairs = json.zipWithIndex.map { case (js, x) => x.toString -> js}
    val shuffledPairs = shuffle(pairs).unsafeRunSync

    pairs != shuffledPairs ==> {
      val result = List(Json(pairs: _*))
      val expected = List(Json(shuffledPairs: _*))

      run(pred, expected, result, fieldOrder, resultOrder) must matcher
    }
  }

  "atLeast" should {
    val pred = AtLeast

    "match exact result with no dupes" >> prop { (a: List[Json]) =>
      val expected = a.distinct
      run(pred, expected, expected, OrderPreserved, OrderIgnored) must beLike { case Success(_, _) => ok }
    }

    "match shuffled result with no dupes" >> prop { (a: List[Json]) =>
      val expected = a.distinct
      val shuffled = shuffle(expected).unsafeRunSync

      shuffled != expected ==> {
        run(pred, expected, shuffled, OrderPreserved, OrderIgnored) must beLike { case Success(_, _) => ok }
      }
    }

    "reject shuffled fields when FieldOrderPreserved" >> prop { (json: List[Json]) =>
      shuffledFields(
        json, pred, OrderPreserved, OrderIgnored,
        beLike { case Failure(msg, _, _, _) => msg must contain("order differs") }
      )
    }

    "accept shuffled fields when FieldOrderIgnored" >> prop { (json: List[Json]) =>
      shuffledFields(
        json, pred, OrderIgnored, OrderIgnored,
        beLike { case Success(_, _) => ok }
      )
    }

    "fail if the process fails" >> prop { (a: List[Json], b: List[Json], c: List[Json]) =>
      val expected = (a ++ b).distinct
      val partialResult = shuffle(a ++ c).unsafeRunSync

      pred(expected, partial(partialResult), OrderPreserved, OrderIgnored).attempt.unsafeRunSync.toOption must beNone
    }

    "match with any additional (no dupes)" >> prop { (a: List[Json], b: List[Json]) =>
      val expected = a.distinct
      val result = shuffle(a ++ b).unsafeRunSync

      b.nonEmpty ==> {
        run(pred, expected, result, OrderPreserved, OrderIgnored) must beLike { case Success(_, _) => ok }
      }
    }

    "reject with any missing (no dupes)" >> prop { (a: List[Json], b: List[Json]) =>
      val expected = (a ++ b).distinct
      val result0 = a.distinct

      expected != result0 ==> {
        val result = shuffle(result0).unsafeRunSync

        run(pred, expected, result, OrderPreserved, OrderIgnored) must beLike {
          case Failure(msg, _, _, _) => msg must contain("unmatched expected values")
        }
      }
    }
  }

  "exactly with result order ignored" should {
    val pred = Exactly

    "match exact result with no dupes" >> prop { (a: List[Json]) =>
      val expected = a.distinct
      run(pred, expected, expected, OrderPreserved, OrderIgnored) must beLike { case Success(_, _) => ok }
    }

    "match shuffled result with no dupes" >> prop { (a: List[Json]) =>
      val expected = a.distinct
      val shuffled = shuffle(expected).unsafeRunSync

      shuffled != expected ==> {
        run(pred, expected, shuffled, OrderPreserved, OrderIgnored) must beLike { case Success(_, _) => ok }
      }
    }

    "reject shuffled fields when field OrderPreserved" >> prop { (json: List[Json]) =>
      shuffledFields(
        json, pred, OrderPreserved, OrderIgnored,
        beLike { case Failure(msg, _, _, _) => msg must contain("order differs") }
      )
    }

    "accept shuffled fields when field OrderIgnored" >> prop { (json: List[Json]) =>
      shuffledFields(
        json, pred, OrderIgnored, OrderIgnored,
        beLike { case Success(_, _) => ok }
      )
    }

    "fail if the process fails" >> prop { (a: List[Json], b: List[Json]) =>
      val expected = (a ++ b).distinct
      val result0 = a.distinct

      expected != result0 ==> {
        val partialResult = shuffle(result0).unsafeRunSync

        pred(expected, partial(partialResult), OrderPreserved, OrderIgnored).attempt.unsafeRunSync.toOption must beNone
      }
    }

    "reject with any additional (no dupes)" >> prop { (a: List[Json], b: List[Json]) =>
      val expected = a.distinct
      val result0 = (a ++ b).distinct

      expected != result0 ==> {
        val result = shuffle(result0).unsafeRunSync

        run(pred, expected, result, OrderPreserved, OrderIgnored) must beLike {
          case Failure(msg, _, _, _) => msg must contain("unexpected value")
        }
      }
    }

    "reject with any missing (no dupes)" >> prop { (a: List[Json], b: List[Json]) =>
      val expected = (a ++ b).distinct
      val result0 = a.distinct

      expected != result0 ==> {
        val result = shuffle(a.distinct).unsafeRunSync

        run(pred, expected, result, OrderPreserved, OrderIgnored) must beLike {
          case Failure(msg, _, _, _) => msg must contain("unmatched expected values")
        }
      }
    }
  }

  "exactly with result order preserved" should {
    val pred = Exactly

    "match exact result" >> prop { (expected: List[Json]) =>
      run(pred, expected, expected, OrderPreserved, OrderPreserved) must beLike { case Success(_, _) => ok }
    }

    "reject shuffled result" >> prop { (expected: List[Json]) =>
      val shuffled = shuffle(expected).unsafeRunSync

      shuffled != expected ==> {
        run(pred, expected, shuffled, OrderPreserved, OrderPreserved) must beLike { case Failure(_, _, _, _) => ok }
      }
    }

    "reject shuffled fields" >> prop { (json: List[Json]) =>
      shuffledFields(
        json, pred, OrderPreserved, OrderPreserved,
        beLike { case Failure(msg, _, _, _) => msg must contain("order differs") }
      )
    }

    "fail if the process fails" >> prop { (a: List[Json], b: List[Json]) =>
      val expected = a ++ b
      val partialResult = a

      pred(expected, partial(partialResult), OrderPreserved, OrderPreserved).attempt.unsafeRunSync.toOption must beNone
    }

    "reject with any additional" >> prop { (a: List[Json], b: List[Json], c: List[Json]) =>
      (b.nonEmpty) ==> {
        val expected = a ++ c
        val result = a ++ b ++ c

        run(pred, expected, result, OrderPreserved, OrderPreserved) must beLike {
          case Failure(msg, _, _, _) =>
            (msg must contain("had more than expected")) or
              (msg must contain("does not match"))
        }
      }
    }

    "reject with any missing" >> prop { (a: List[Json], b: List[Json], c: List[Json]) =>
      (b.nonEmpty) ==> {
        val expected = a ++ b ++ c
        val result = a ++ c

        run(pred, expected, result, OrderPreserved, OrderPreserved) must beLike {
          case Failure(msg, _, _, _) =>
            (msg must contain("ran out before expected")) or
              (msg must contain("does not match"))
        }
      }
    }
  }

  "initial with result order preserved" should {
    val pred = Initial

    "match exact result" >> prop { (expected: List[Json]) =>
      run(pred, expected, expected, OrderPreserved, OrderPreserved) must beLike { case Success(_, _) => ok }
    }

    "reject shuffled result" >> prop { (expected: List[Json]) =>
      val shuffled = shuffle(expected).unsafeRunSync

      shuffled != expected ==> {
        run(pred, expected, shuffled, OrderPreserved, OrderPreserved) must beLike { case Failure(msg, _, _, _) => msg must contain("does not match") }
      }
    }

    "reject shuffled fields" >> prop { (json: List[Json]) =>
      shuffledFields(
        json, pred, OrderPreserved, OrderPreserved,
        beLike { case Failure(msg, _, _, _) => msg must contain("order differs") }
      )
    }

    "fail if the process fails" >> prop { (a: List[Json], b: List[Json]) =>
      val expected = a ++ b
      val partialResult = partial(a)

      pred(expected, partialResult, OrderPreserved, OrderPreserved).attempt.unsafeRunSync.toOption must beNone
    }

    "match with any following" >> prop { (a: List[Json], b: List[Json]) =>
      val expected = a
      val result = a ++ b

      run(pred, expected, result, OrderPreserved, OrderPreserved) must beLike { case Success(_, _) => ok }
    }

    "reject with any leading" >> prop { (a: List[Json], b: List[Json], c: List[Json]) =>
      val expected = b
      val result = a ++ b ++ c
      (a.nonEmpty && b.nonEmpty && result.take(b.length) != b) ==> {
        run(pred, expected, result, OrderPreserved, OrderPreserved) must beLike { case Failure(msg, _, _, _) => msg must contain("does not match") }
      }
    }
  }

  "doesNotContain" should {
    val pred = DoesNotContain

    "reject exact result with no dupes" >> prop { (a: List[Json]) =>
      val expected = a.distinct
      run(pred, expected, expected, OrderPreserved, OrderIgnored) must beLike { case Failure(_, _, _, _) => ok }
    }

    "reject shuffled result with no dupes" >> prop { (a: List[Json]) =>
      val expected = a.distinct
      val shuffled = shuffle(expected).unsafeRunSync

      shuffled != expected ==> {
        run(pred, expected, shuffled, OrderPreserved, OrderIgnored) must beLike { case Failure(msg, _, _, _) => msg must contain("prohibited values") }
      }
    }

    "reject shuffled fields" >> prop { (json: List[Json]) =>
      shuffledFields(
        json, pred, OrderPreserved, OrderIgnored,
        beLike { case Failure(msg, _, _, _) => msg must contain("prohibited values") }
      )
    }

    "fail if the process fails" >> prop { (result: List[Json], expected: List[Json]) =>
      (result.toSet intersect expected.toSet).isEmpty ==> {
        pred(expected, partial(result), OrderPreserved, OrderIgnored).attempt.unsafeRunSync.toOption must beNone
      }
    }.set(maxSize = 10)

    "reject any subset" >> prop { (a: List[Json], b: List[Json], c: List[Json]) =>
      b.nonEmpty ==> {
        val expected = shuffle(b ++ c).unsafeRunSync
        val result = shuffle(a ++ b).unsafeRunSync

        run(pred, expected, result, OrderPreserved, OrderIgnored) must beLike { case Failure(_, _, _, _) => ok }
      }
    }

    "match any disjoint values" >> prop { (result: List[Json], expected: List[Json]) =>
      (result.toSet intersect expected.toSet).isEmpty ==> {
        val values: Stream[IO, Json] = Stream.emits(result)

        if (expected.nonEmpty)
          run(pred, expected, result, OrderPreserved, OrderIgnored) must beLike { case Success(_, _) => ok }
        else
          run(pred, expected, result, OrderPreserved, OrderIgnored) must beLike { case Failure(_, _, _, _) => ok }
      }
    }.set(maxSize = 10)
  }
}

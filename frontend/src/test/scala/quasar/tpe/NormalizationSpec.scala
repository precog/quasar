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

package quasar.tpe

import quasar.Qspec
import quasar.contrib.matryoshka._
import quasar.contrib.matryoshka.arbitrary._
import quasar.ejson.{EJson, EJsonArbitrary}
import quasar.ejson.implicits._
import quasar.fp._, Helpers._
import quasar.contrib.iota._

import scala.Predef.$conforms

import algebra.PartialOrder
import matryoshka.data.Fix
import matryoshka.implicits._
import org.specs2.scalacheck._
import scalaz._, Scalaz._
import scalaz.scalacheck.ScalazArbitrary._

final class NormalizationSpec extends Qspec with TypeFArbitrary with EJsonArbitrary {
  // NB: Limit type depth to something reasonable.
  implicit val params = Parameters(maxSize = 10)

  import TypeF._, normalization._

  type J = Fix[EJson]
  type T = Fix[TypeF[J, ?]]

  val isUnion = project[T, TypeF[J, ?]].composePrism(union[J, T]).exist(_ => true)(_)

  "coalesceUnion" >> {
    "resulting union contains no unions" >> prop { (x: T, y: T, zs: IList[T]) =>
      Unioned.unapply(coalesceUnion[J, T] apply union[J, T](x, y, zs))
        .map(_.any(isUnion)) must beSome(beFalse)
    }
  }

  "elideBottom" >> {
    "resulting union does not contain bottom" >> prop { zs: IList[T] =>
      val u = union[J, T](top[J, T]().embed, bottom[J, T]().embed, zs)
      (elideBottom[J, T] <<< coalesceUnion[J, T])(u) must beLike {
        case Unioned(ts) => ts.any(isBottom[J](_)) must beFalse
        case _           => ok
      }
    }
  }

  "reduceToTop" >> {
    "unions containing top become top" >> prop { (x: T, xs: IList[T]) =>
      top[J, T].nonEmpty(
        reduceToTop[J, T] apply union[J, T](x, top[J, T]().embed, xs)
      ) must beTrue
    }
  }

  "simplifyUnion" >> {
    "resulting union is disjoint" >> prop { (x: T, y: T, zs: IList[T]) =>
      ((simplifyUnion[J, T] <<< coalesceUnion[J, T]) apply union[J, T](x, y, zs)) must beLike {
        case Unioned(ts) => ts.all(t => !ts.any(PartialOrder[T].lt(_, t))) must beTrue
        case _           => ok
      }
    }
  }
}

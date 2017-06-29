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

package quasar.yggdrasil

import quasar.blueeyes._, json._

import scalaz._, Scalaz._

import scala.collection.mutable
import scala.collection.generic.CanBuildFrom
import scala.util.Random

import org.specs2._
import org.scalacheck._
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary._
import CValueGenerators.JSchema

case class SampleData(data: Stream[JValue], schema: Option[(Int, JSchema)] = None) {
  override def toString = {
    "SampleData: \ndata = "+data.map(_.toString.replaceAll("\n", "\n  ")).mkString("[\n  ", ",\n  ", "]\n") +
    "\nschema: " + schema
  }

  def sortBy[B: scala.math.Ordering](f: JValue => B) = copy(data = data.sortBy(f))
}

object SampleData extends CValueGenerators {
  def toRecord(ids: Array[Long], jv: JValue): JValue = {
    JObject(Nil).set(JPath(".key"), JArray(ids.map(JNum(_)).toList)).set(JPath(".value"), jv)
  }

  implicit def keyOrder[A]: scala.math.Ordering[(Identities, A)] = tupledIdentitiesOrder[A](IdentitiesOrder).toScalaOrdering

  def sample(schema: Int => Gen[JSchema]) = Arbitrary(
    for {
      depth   <- choose(0, 1)
      jschema <- schema(depth)
      (idCount, data) <- genEventColumns(jschema)
    }
    yield {
      SampleData(
        data.sorted.toStream flatMap {
          // Sometimes the assembly process will generate overlapping values which will
          // cause RuntimeExceptions in JValue.unsafeInsert. It's easier to filter these
          // out here than prevent it from happening in the first place.
          case (ids, jv) => try { Some(toRecord(ids, assemble(jv))) } catch { case _ : RuntimeException => None }
        },
        Some((idCount, jschema))
      )
    }
  )

  def distinctBy[T, C[X] <: Seq[X], S](c: C[T])(key: T => S)(implicit cbf: CanBuildFrom[C[T], T, C[T]]): C[T] = {
    val builder = cbf()
    val seen = mutable.HashSet[S]()

    for (t <- c) {
      if (!seen(key(t))) {
        builder += t
        seen += key(t)
      }
    }

    builder.result
  }

  def randomSubset[T, C[X] <: Seq[X], S](c: C[T], freq: Double)(implicit cbf: CanBuildFrom[C[T], T, C[T]]): C[T] = {
    val builder = cbf()

    for (t <- c)
      if (Random.nextDouble < freq)
        builder += t

    builder.result
  }

  def sort(sample: Arbitrary[SampleData]): Arbitrary[SampleData] = {
    Arbitrary(
      for {
        sampleData <- arbitrary(sample)
      } yield {
        SampleData(sampleData.data.sorted, sampleData.schema)
      }
    )
  }

  def shuffle(sample: Arbitrary[SampleData]): Arbitrary[SampleData] = {
    val gen =
      for {
        sampleData <- arbitrary(sample)
      } yield {
        SampleData(Random.shuffle(sampleData.data), sampleData.schema)
      }

    Arbitrary(gen)
  }

  def distinct(sample: Arbitrary[SampleData]) : Arbitrary[SampleData] = {
    Arbitrary(
      for {
        sampleData <- arbitrary(sample)
      } yield {
        SampleData(sampleData.data.distinct, sampleData.schema)
      }
    )
  }

  def distinctKeys(sample: Arbitrary[SampleData]) : Arbitrary[SampleData] = {
    Arbitrary(
      for {
        sampleData <- arbitrary(sample)
      } yield {
        SampleData(distinctBy(sampleData.data)(_ \ "keys"), sampleData.schema)
      }
    )
  }

  def distinctValues(sample: Arbitrary[SampleData]) : Arbitrary[SampleData] = {
    Arbitrary(
      for {
        sampleData <- arbitrary(sample)
      } yield {
        SampleData(distinctBy(sampleData.data)(_ \ "value"), sampleData.schema)
      }
    )
  }

  def duplicateRows(sample: Arbitrary[SampleData]): Arbitrary[SampleData] = {
    val gen =
      for {
        sampleData <- arbitrary(sample)
      } yield {
        val rows = sampleData.data
        val duplicates = randomSubset(rows, 0.25)
        SampleData(Random.shuffle(rows ++ duplicates), sampleData.schema)
      }

    Arbitrary(gen)
  }

  def undefineRows(sample: Arbitrary[SampleData]): Arbitrary[SampleData] = {
    val gen =
      for {
        sampleData <- arbitrary(sample)
      } yield {
        val rows = for(row <- sampleData.data)
          yield if (Random.nextDouble < 0.25) JUndefined else row
        SampleData(rows, sampleData.schema)
      }

    Arbitrary(gen)
  }

  def undefineRowsForColumn(sample: Arbitrary[SampleData], path: JPath): Arbitrary[SampleData] = {
    val gen = for {
      sampleData <- arbitrary(sample)
    } yield {
      val rows = for (row <- sampleData.data) yield {
        if (false && Random.nextDouble >= 0.25) {
          row
        } else if (row.get(path) != null && row.get(path) != JUndefined) {
          row.set(path, JUndefined)
        } else {
          row
        }
      }
      SampleData(rows, sampleData.schema)
    }

    Arbitrary(gen)
  }
}




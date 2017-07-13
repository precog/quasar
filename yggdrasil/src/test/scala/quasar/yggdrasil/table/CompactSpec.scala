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

package quasar.yggdrasil.table

import quasar.precog.BitSet
import quasar.precog.TestSupport._
import quasar.precog.common._
import quasar.precog.util._
import quasar.yggdrasil._

import scala.util.Random
import scalaz._, Scalaz._

trait CompactSpec[M[+_]] extends ColumnarTableModuleTestSupport[M] with SpecificationLike with ScalaCheck {
  import SampleData._
  import trans._

  def tableStats(table: Table) : List[(Int, Int)] = table match {
    case cTable: ColumnarTable =>
      val slices = cTable.slices.toStream.copoint
      val sizes = slices.map(_.size).toList
      val undefined = slices.map { slice =>
        (0 until slice.size).foldLeft(0) {
          case (acc, i) => if(!slice.columns.values.exists(_.isDefinedAt(i))) acc+1 else acc
        }
      }.toList

      sizes zip undefined
  }

  def mkDeref(path: CPath): TransSpec1 = {
    def mkDeref0(nodes: List[CPathNode]): TransSpec1 = nodes match {
      case (f : CPathField) :: rest => DerefObjectStatic(mkDeref0(rest), f)
      case (i : CPathIndex) :: rest => DerefArrayStatic(mkDeref0(rest), i)
      case _ => Leaf(Source)
    }

    mkDeref0(path.nodes)
  }

  def extractPath(spec: TransSpec1): Option[CPath] = spec match {
    case DerefObjectStatic(TransSpec1.Id, f) => Some(f)
    case DerefObjectStatic(lhs, f) => extractPath(lhs).map(_ \ f)
    case DerefArrayStatic(TransSpec1.Id, i) => Some(i)
    case DerefArrayStatic(lhs, f) => extractPath(lhs).map(_ \ f)
    case _ => None
  }

  def chooseColumn(table: Table): TransSpec1 = table match {
    case cTable: ColumnarTable =>
      cTable.slices.toStream.copoint.headOption.map { slice =>
        val chosenPath = Random.shuffle(slice.columns.keys.map(_.selector)).head
        mkDeref(chosenPath)
      } getOrElse(mkDeref(CPath.Identity))
  }

  def undefineTable(fullTable: Table): Table = fullTable match {
    case cTable: ColumnarTable =>
      val slices = cTable.slices.toStream.copoint // fuzzing must be done strictly otherwise sadness will ensue
      val numSlices = slices.size

      val maskedSlices = slices.map { slice =>
        if(numSlices > 1 && Random.nextDouble < 0.25) {
          new Slice {
            val size = slice.size
            val columns = slice.columns.mapValues { col => (col |> cf.util.filter(0, slice.size, new BitSet)).get }
          }
        } else {
          val retained = (0 until slice.size).flatMap {
            x => if (scala.util.Random.nextDouble < 0.75) Some(x) else None
          }
          new Slice {
            val size = slice.size
            val columns = slice.columns.mapValues {
              col => (col |> cf.util.filter(0, slice.size, BitSetUtil.create(retained))).get
            }
          }
        }
      }

      Table(StreamT.fromStream(M.point(maskedSlices)), UnknownSize)
  }

  def undefineColumn(fullTable: Table, path: CPath): Table = fullTable match {
    case cTable: ColumnarTable =>
      val slices = cTable.slices.toStream.copoint // fuzzing must be done strictly otherwise sadness will ensue
      val numSlices = slices.size

      val maskedSlices = slices.map { slice =>
        val colRef = slice.columns.keys.find(_.selector == path)
        val maskedSlice = colRef.map { colRef =>
          val col = slice.columns(colRef)
          val maskedCol =
            if(numSlices > 1 && Random.nextDouble < 0.25)
              (col |> cf.util.filter(0, slice.size, new BitSet)).get
            else {
              val retained = (0 until slice.size).map { (x : Int) => if(scala.util.Random.nextDouble < 0.75) Some(x) else None }.flatten
              (col |> cf.util.filter(0, slice.size, BitSetUtil.create(retained))).get
            }
          new Slice {
            val size = slice.size
            val columns = slice.columns.updated(colRef, maskedCol)
          }
        }
        maskedSlice.getOrElse(slice)
      }

      Table(StreamT.fromStream(M.point(maskedSlices)), UnknownSize)
  }

  def testCompactIdentity = {
    implicit val gen = sample(schema)
    prop { (sample: SampleData) =>
      val table = fromSample(sample)
      val compactTable = table.compact(Leaf(Source))

      val results = toJson(compactTable)

      results.copoint must_== sample.data
    }
  }

  def testCompactPreserve = {
    implicit val gen = sample(schema)
    prop { (sample: SampleData) =>
      val sampleTable = undefineTable(fromSample(sample))
      val sampleJson = toJson(sampleTable)

      val compactTable = sampleTable.compact(Leaf(Source))
      val results = toJson(compactTable)

      results.copoint must_== sampleJson.copoint
    }
  }

  def testCompactRows = {
    implicit val gen = sample(schema)
    prop { (sample: SampleData) =>
      val sampleTable = undefineTable(fromSample(sample))
      val sampleJson = toJson(sampleTable)

      val compactTable = sampleTable.compact(Leaf(Source))
      val compactStats = tableStats(compactTable)
      val results = toJson(compactTable)

      compactStats.map(_._2).foldLeft(0)(_+_) must_== 0
    }
  }

  def testCompactSlices = {
    implicit val gen = sample(schema)
    prop { (sample: SampleData) =>
      val sampleTable = undefineTable(fromSample(sample))
      val sampleJson = toJson(sampleTable)

      val compactTable = sampleTable.compact(Leaf(Source))
      val compactStats = tableStats(compactTable)
      val results = toJson(compactTable)

      compactStats.map(_._1).count(_ == 0) must_== 0
    }
  }

  def testCompactPreserveKey = {
    implicit val gen = sample(schema)
    prop { (sample: SampleData) =>
      val baseTable = fromSample(sample)
      val key = chooseColumn(baseTable)

      val sampleTable = undefineColumn(baseTable, extractPath(key).getOrElse(CPath.Identity))
      val sampleKey = sampleTable.transform(key)
      val sampleKeyJson = toJson(sampleKey)

      val compactTable = sampleTable.compact(key)
      val resultKey = compactTable.transform(key)
      val resultKeyJson = toJson(resultKey)

      resultKeyJson.copoint must_== sampleKeyJson.copoint
    }
  }

  def testCompactRowsKey = {
    implicit val gen = sample(schema)
    prop { (sample: SampleData) =>
      val baseTable = fromSample(sample)
      val key = chooseColumn(baseTable)

      val sampleTable = undefineColumn(baseTable, extractPath(key).getOrElse(CPath.Identity))
      val sampleKey = sampleTable.transform(key)

      val compactTable = sampleTable.compact(key)
      val resultKey = compactTable.transform(key)
      val resultKeyStats = tableStats(resultKey)

      resultKeyStats.map(_._2).foldLeft(0)(_+_) must_== 0
    }
  }

  def testCompactSlicesKey = {
    implicit val gen = sample(schema)
    prop { (sample: SampleData) =>
      val baseTable = fromSample(sample)
      val key = chooseColumn(baseTable)

      val sampleTable = undefineColumn(baseTable, extractPath(key).getOrElse(CPath.Identity))
      val sampleKey = sampleTable.transform(key)

      val compactTable = sampleTable.compact(key)
      val resultKey = compactTable.transform(key)
      val resultKeyStats = tableStats(resultKey)

      resultKeyStats.map(_._1).count(_ == 0) must_== 0
    }
  }
}

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

package quasar.qscript.analysis

import slamdata.Predef._
import quasar.fp.ski.κ
import quasar.contrib.pathy.{AFile, ADir, APath}
import quasar.qscript._
import quasar.qscript.MapFuncsCore._
import quasar.common.{JoinType, SortDir}


import matryoshka.data.Fix
import pathy.Path._
import org.specs2.scalaz.DisjunctionMatchers
import scalaz._, Scalaz._

class CardinalitySpec extends quasar.Qspec with QScriptHelpers with DisjunctionMatchers {

  sequential

  val empty: APath => Id[Int] = κ(0)

  "Cardinality" should {

    "Read" should {
      "always returns 1 for any file" in {
        val compile = Cardinality.read[AFile].calculate(empty)
        val afile = rootDir </> dir("path") </> dir("to") </> file("file")
        compile(Const[Read[AFile], Int](Read(afile))) must_== 1
      }

      "always returns 1 for any dir" in {
        val compile = Cardinality.read[ADir].calculate(empty)
        val adir = rootDir </> dir("path") </> dir("to") </> dir("dir")
        compile(Const[Read[ADir], Int](Read(adir))) must_== 1
      }
    }

    "ShiftedRead" should {
      "returns what 'pathCard' is returning for given file" in {
        val fileCardinality = 50
        val pathCard = κ(fileCardinality)
        val compile = Cardinality.shiftedRead[AFile].calculate[Id](pathCard)
        val afile = rootDir </> dir("path") </> dir("to") </> file("file")
        compile(Const[ShiftedRead[AFile], Int](ShiftedRead(afile, ExcludeId))) must_== fileCardinality
      }

      "returns what 'pathCard' is returning for given dir" in {
        val dirCardinality = 55
        val pathCard = κ(dirCardinality)
        val compile = Cardinality.shiftedRead[ADir].calculate[Id](pathCard)
        val adir = rootDir </> dir("path") </> dir("to") </> dir("dir")
        compile(Const[ShiftedRead[ADir], Int](ShiftedRead(adir, ExcludeId))) must_== dirCardinality
      }
    }

    "QScriptCore" should {
      val compile = Cardinality.qscriptCore[Fix].calculate(empty)
      "Map" should {
        "returns cardinality of already processed part of qscript" in {
          val cardinality = 40
          val map = quasar.qscript.Map(cardinality, ProjectFieldR(HoleF, StrLit("field")))
          compile(map) must_== cardinality
        }
      }
      /**
        *  Cardinality depends on how many buckets there are created. If it is Constant
        *  then cardinality == 1 otherwise it is something in range [0, card]. Middle
        *  value is chosen however same as with Filter we might consider returning
        *  a Tuple2[Int, Int] as a range of values instead of Int
        */
      "Reduce" should {
        "returns cardinality of 1 when bucket is Const" in {
          val bucket: FreeMap = Free.liftF(MFC(MapFuncCore.EmptyArray.apply))
          val repair: Free[MapFunc, ReduceIndex] = Free.point(ReduceIndex(0.some))
          val reduce = Reduce(100, bucket, List.empty, repair)
          compile(reduce) must_== 1
        }
        "returns cardinality of half fo already processed part of qscript" in {
          val cardinality = 100
          val bucket: FreeMap = ProjectFieldR(HoleF, StrLit("country"))
          val repair: Free[MapFunc, ReduceIndex] = Free.point(ReduceIndex(0.some))
          val reduce = Reduce(cardinality, bucket, List.empty, repair)
          compile(reduce) must_== cardinality / 2
        }
      }
      "Sort" should {
        "returns cardinality of already processed part of qscript" in {
          val cardinality = 60
          def bucket = ProjectFieldR(HoleF, StrLit("field"))
          def order = (bucket, SortDir.asc).wrapNel
          val sort = quasar.qscript.Sort(cardinality, bucket, order)
          compile(sort) must_== cardinality
        }
      }
      "Filter" should {
        /**
          * Since filter can return cardinality of a range [0, card] a middle value
          * was chosen - card / 2.
          * It is worth considering changing signature of Cardinality typeclass to
          * return Tuple2[Int, Int] representing range. Then the result would be
          * range (0, card)
          */
        "returns half of cardinality of already processed part of qscript" in {
          val cardinality = 50
          def func: FreeMap = Free.roll(MFC(Lt(ProjectFieldR(HoleF, StrLit("age")), IntLit(24))))
          val filter = quasar.qscript.Filter(cardinality, func)
          compile(filter) must_== cardinality / 2
        }
      }
      "Subset" should {
        "returns cardinality equal to count if sel is Take & count is constant" in {
          val count = 20
          val cardinality = 50
          def fromQS: FreeQS = Free.point(SrcHole)
          def countQS: FreeQS = constFreeQS(count)

          val take = quasar.qscript.Subset(cardinality, fromQS, Take, countQS)
          compile(take) must_== count
        }
        "returns cardinality equal to count if sel is Sample & count is constant" in {
          val count = 20
          val cardinality = 50
          def fromQS: FreeQS = Free.point(SrcHole)
          def countQS: FreeQS = constFreeQS(count)

          val take = quasar.qscript.Subset(cardinality, fromQS, Sample, countQS)
          compile(take) must_== count
        }
        "returns cardinality equal to (card - count) if sel is Drop & count is constant" in {
          val count = 20
          val cardinality = 50
          def fromQS: FreeQS = Free.point(SrcHole)
          def countQS: FreeQS = constFreeQS(count)

          val take = quasar.qscript.Subset(cardinality, fromQS, Drop, countQS)
          compile(take) must_== cardinality - count
        }
        "returns cardinality equal to card / 2 regardles of sel if count is NOT constant" in {
          val cardinality = 50
          def fromQS: FreeQS = Free.point(SrcHole)
          def countQS: FreeQS = Free.point(SrcHole)

          compile(quasar.qscript.Subset(cardinality, fromQS, Take, countQS)) must_== cardinality / 2
          compile(quasar.qscript.Subset(cardinality, fromQS, Sample, countQS)) must_== cardinality / 2
          compile(quasar.qscript.Subset(cardinality, fromQS, Drop, countQS)) must_== cardinality / 2
        }
      }
      "LeftShift" should {
        /**
          * Question why 10x not 5x or 1000x ?
          * LeftShifts flattens the structure. Thus the range has potentail size
          * from [cardinality, infinity]. It is really hard to determin a concrete value
          * just by spectating a static information. To get more accurate data we will
          * most probably need some statistics.
          * Other approach is to change the Cardinality typeclss to return Option[Int]
          * and all occurance of LeftShift would return None
          * For now the x10 approach was proposed as a value.
          */
        "returns cardinality of 10 x cardinality of already processed part of qscript" in {
          val cardinality = 60
          val func: FreeMap =
            Free.roll(MFC(MapFuncsCore.Eq(ProjectFieldR(HoleF, StrLit("field")), StrLit("value"))))
          val joinFunc: JoinFunc = (LeftSide : JoinSide).point[Free[MapFunc, ?]]
          val leftShift = LeftShift(cardinality, func, IdOnly, joinFunc)
          compile(leftShift) must_== cardinality * 10
        }
      }
      "Union" should {
        "returns cardinality of sum lBranch + rBranch" in {
          val cardinality = 100
          def func(country: String): FreeMap =
            Free.roll(MFC(MapFuncsCore.Eq(ProjectFieldR(HoleF, StrLit("country")), StrLit(country))))
          def left: FreeQS = Free.roll(QCT.inj(quasar.qscript.Map(HoleQS, ProjectFieldR(HoleF, StrLit("field")))))
          def right: FreeQS = Free.roll(QCT.inj(Filter(HoleQS, func("US"))))
          val union = quasar.qscript.Union(cardinality, left, right)
          compile(union) must_== cardinality + (cardinality / 2)
        }
      }
      "Unreferenced" should {
        "returns cardinality of 1" in {
          compile(Unreferenced()) must_== 1
        }
      }
    }

    "ProjectBucket"  should {
      "returns cardinality of already processed part of qscript" in {
        val compile = Cardinality.projectBucket[Fix].calculate(empty)
        val cardinality = 45
        def func: FreeMap = Free.roll(MFC(Lt(ProjectFieldR(HoleF, StrLit("age")), IntLit(24))))
        val bucket = BucketField(cardinality, func, func)
        compile(bucket) must_== cardinality
      }
    }

    "EquiJoin" should {
      "returns cardinality of multiplication lBranch * rBranch" in {
        val compile = Cardinality.equiJoin[Fix].calculate(empty)
        val cardinality = 100
        val func: FreeMap =
          Free.roll(MFC(MapFuncsCore.Eq(ProjectFieldR(HoleF, StrLit("field")), StrLit("val"))))
        def left: FreeQS = Free.roll(QCT.inj(quasar.qscript.Map(HoleQS, ProjectFieldR(HoleF, StrLit("field")))))
        def right: FreeQS = Free.roll(QCT.inj(Filter(HoleQS, func)))
        val joinFunc: JoinFunc = (LeftSide : JoinSide).point[Free[MapFunc, ?]]
        val join = quasar.qscript.EquiJoin(cardinality, left, right, func, func, JoinType.Inner, joinFunc)
        compile(join) must_== cardinality * (cardinality / 2)
      }
    }

    "ThetaJoin" should {
      "return cardinality of multiplication lBranch * rBranch" in {
        val compile = Cardinality.thetaJoin[Fix].calculate(empty)
        val cardinality = 100
        val func: FreeMap =
          Free.roll(MFC(MapFuncsCore.Eq(ProjectFieldR(HoleF, StrLit("field")), StrLit("val"))))
        def left: FreeQS = Free.roll(QCT.inj(quasar.qscript.Map(HoleQS, ProjectFieldR(HoleF, StrLit("field")))))
        def right: FreeQS = Free.roll(QCT.inj(Filter(HoleQS, func)))
        val joinFunc: JoinFunc = (LeftSide : JoinSide).point[Free[MapFunc, ?]]
        val join = quasar.qscript.ThetaJoin(cardinality, left, right, joinFunc, JoinType.Inner, joinFunc)
        compile(join) must_== cardinality * (cardinality / 2)
      }
    }

    "DeadEnd" should {
      "return cardinality of 1" in {
        val compile = Cardinality.deadEnd.calculate(empty)
        compile(Const(Root)) must_== 1
      }
    }
  }

  private def constFreeQS(v: Int): FreeQS =
    Free.roll(QCT.inj(quasar.qscript.Map(Free.roll(QCT.inj(Unreferenced())), IntLit(v))))
}

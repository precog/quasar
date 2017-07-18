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

package quasar.physical.sparkcore.fs

import slamdata.Predef._
import quasar._, quasar.Planner._
import quasar.common.SortDir
import quasar.contrib.pathy.AFile
import quasar.fp._, ski._
import quasar.qscript._, ReduceFuncs._, SortDir._

import scala.math.{Ordering => SOrdering}, SOrdering.Implicits._

import org.apache.spark._
import org.apache.spark.rdd._
import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

class QScriptCorePlanner[T[_[_]]: BirecursiveT: ShowT]
    extends Planner[QScriptCore[T, ?]] {

  import Planner.{SparkState, SparkStateT}

  type Index = Long
  type Count = Long

  // TODO consider moving to data.scala (conflicts with existing code)
  @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
  implicit def dataOrder: Order[Data] = new Order[Data] with Serializable {
    def order(d1: Data, d2: Data) = (d1, d2) match {
      case Data.Null -> Data.Null                 => Ordering.EQ
      case Data.Str(a) -> Data.Str(b)             => a cmp b
      case Data.Bool(a) -> Data.Bool(b)           => a cmp b
      case Data.Number(a) -> Data.Number(b)       => a cmp b
      case Data.Obj(a) -> Data.Obj(b)             => a.toList cmp b.toList
      case Data.Arr(a) -> Data.Arr(b)             => a cmp b
      case Data.Set(a) -> Data.Set(b)             => a cmp b
      case Data.Timestamp(a) -> Data.Timestamp(b) => Ordering fromInt (a compareTo b)
      case Data.Date(a) -> Data.Date(b)           => Ordering fromInt (a compareTo b)
      case Data.Time(a) -> Data.Time(b)           => Ordering fromInt (a compareTo b)
      case Data.Interval(a) -> Data.Interval(b)   => Ordering fromInt (a compareTo b)
      case Data.Binary(a) -> Data.Binary(b)       => a.toArray.toList cmp b.toArray.toList
      case Data.Id(a) -> Data.Id(b)               => a cmp b
      case Data.NA -> Data.NA                 => Ordering.EQ
      case a -> b                       => a.getClass.## cmp b.getClass.##
    }
  }

  /*
   * Copy-paste from scalaz's `toScalaOrdering`
   * Copied because scalaz's ListInstances are not Serializable
   */
  implicit val ord: SOrdering[Data] = new SOrdering[Data] {
    def compare(x: Data, y: Data) = dataOrder.order(x, y).toInt
  }


  private def filterOut(
    fromFile: (SparkContext, AFile) => Task[RDD[Data]],
    src: RDD[Data],
    from: FreeQS[T],
    count: FreeQS[T],
    predicate: (Index, Count) => Boolean ):
      StateT[EitherT[Task, PlannerError, ?], SparkContext, RDD[Data]] = {

    val algebraM = Planner[QScriptTotal[T, ?]].plan(fromFile)
    val srcState = src.point[SparkState]

    val fromState: SparkState[RDD[Data]] = from.cataM(interpretM(κ(srcState), algebraM))
    val countState: SparkState[RDD[Data]] = count.cataM(interpretM(κ(srcState), algebraM))

    val countEval: SparkState[Long] = countState >>= (rdd => EitherT(Task.delay(rdd.first match {
      case Data.Int(v) if v.isValidLong => v.toLong.right[PlannerError]
      case Data.Int(v) => InternalError.fromMsg(s"Provided Integer $v is not a Long").left[Long]
      case a => InternalError.fromMsg(s"$a is not a Long number").left[Long]
    })).liftM[StateT[?[_], SparkContext, ?]])
    (fromState |@| countEval)((rdd, count) =>
      rdd.zipWithIndex.filter(di => predicate(di._2, count)).map(_._1))
  }

  private def reduceData: ReduceFunc[_] => (Data, Data) => Data = {
    case Count(_) => (d1: Data, d2: Data) => (d1, d2) match {
      case (Data.Int(a), Data.Int(b)) => Data.Int(a + b)
      case _ => Data.NA
    }
    case Sum(_) => (d1: Data, d2: Data) => (d1, d2) match {
      case (Data.Int(a), Data.Int(b)) => Data.Int(a + b)
      case (Data.Number(a), Data.Number(b)) => Data.Dec(a + b)
      case _ => Data.NA
    }
    case Min(_) => (d1: Data, d2: Data) => (d1, d2) match {
      case (Data.Int(a), Data.Int(b)) => Data.Int(a.min(b))
      case (Data.Number(a), Data.Number(b)) => Data.Dec(a.min(b))
      case (Data.Str(a), Data.Str(b)) => Data.Str((a.compare(b) < 0).fold(a, b))
      case _ => Data.NA
    }
    case Max(_) => (d1: Data, d2: Data) => (d1, d2) match {
      case (Data.Int(a), Data.Int(b)) => Data.Int(a.max(b))
      case (Data.Number(a), Data.Number(b)) => Data.Dec(a.max(b))
      case (Data.Str(a), Data.Str(b)) => Data.Str((a.compare(b) > 0).fold(a, b))
      case _ => Data.NA
    }
    case Avg(_) => (d1: Data, d2: Data) => (d1, d2) match {
      case (Data.Arr(List(Data.Dec(s1), Data.Int(c1))), Data.Arr(List(Data.Dec(s2), Data.Int(c2)))) =>
        Data.Arr(List(Data.Dec(s1 + s2), Data.Int(c1 + c2)))
    }
    case Arbitrary(_) => (d1: Data, d2: Data) => d1
    case First(_)     => (d1: Data, d2: Data) => d1
    case Last(_)      => (d1: Data, d2: Data) => d2
    case UnshiftArray(a) => (d1: Data, d2: Data) => (d1, d2) match {
      case (Data.Arr(a), Data.Arr(b)) => Data.Arr(a ++ b)
      case _ => Data.NA
    }
    case UnshiftMap(a1, a2) => (d1: Data, d2: Data) => (d1, d2) match {
      case (Data.Obj(a), Data.Obj(b)) => Data.Obj(a ++ b)
      case _ => Data.NA
    }

  }

  def plan(fromFile: (SparkContext, AFile) => Task[RDD[Data]]): AlgebraM[SparkState, QScriptCore[T, ?], RDD[Data]] = {
    case qscript.Map(src, f) =>
      StateT((sc: SparkContext) =>
        EitherT(CoreMap.changeFreeMap(f).map(df => (sc, src.map(df))).point[Task]))
    case Reduce(src, bucket, reducers, repair) =>
      val maybePartitioner: PlannerError \/ (Data => Data) =
        CoreMap.changeFreeMap(bucket)

      val extractFunc: ReduceFunc[Data => Data] => (Data => Data) = {
        case Count(a) => a >>> {
          case Data.NA => Data.Int(0)
          case _ => Data.Int(1)
        }
        case Sum(a) => a
        case Min(a) => a
        case Max(a) => a
        case Avg(a) => a >>> {
          case Data.Int(v) => Data.Arr(List(Data.Dec(BigDecimal(v)), Data.Int(1)))
          case Data.Dec(v) => Data.Arr(List(Data.Dec(v.apply(BigDecimal.defaultMathContext)), Data.Int(1)))
          case _ => Data.NA
        }
        case Arbitrary(a) => a
        case First(a) => a
        case Last(a) => a
        case UnshiftArray(a) => a >>> ((d: Data) => Data.Arr(List(d)))
        case UnshiftMap(a1, a2) => ((d: Data) => a1(d) match {
          case Data.Str(k) => Data.Obj(ListMap(k -> a2(d)))
          case Data.Int(i) => Data.Obj(ListMap(i.shows -> a2(d)))
          case _ => Data.NA
        })
      }

      val maybeTransformers: PlannerError \/ List[Data => Data] =
        reducers.traverse(_.traverse(CoreMap.changeFreeMap[T]).map(extractFunc))

      val reducersFuncs: List[(Data,Data) => Data] =
        reducers.map(reduceData)

      val maybeRepair: PlannerError \/ ((Data, List[Data]) => Data) =
        CoreMap.changeReduceFunc(repair)

      def merge
        (a: List[Data], b: List[Data], f: List[(Data, Data) => Data])
          : List[Data] =
        Zip[List].zipWith(f,(a.zip(b)))(_.tupled(_))

      val avgReducers = reducers.map {
        case Avg(_) => true
        case _  => false
      }

      StateT((sc: SparkContext) =>
        EitherT(((maybePartitioner |@| maybeTransformers |@| maybeRepair) {
          case (partitioner, trans, repair) =>
            src.map(d => (partitioner(d), trans.map(_(d))))
              .reduceByKey(merge(_,_, reducersFuncs))
              .map {
                case (k, vs) =>
                  val v = Zip[List].zipWith(vs, avgReducers) {
                    case (Data.Arr(List(Data.Dec(sum), Data.Int(count))), true) => Data.Dec(sum / BigDecimal(count))
                    case (d, _) => d
                  }
                  repair(k, v)
              }
        }).map((sc, _)).point[Task])
      )
    case Sort(src, bucket, orders) =>

      val maybeSortBys: PlannerError \/ NonEmptyList[(Data => Data, SortDir)] =
        orders.traverse {
          case (freemap, sdir) => CoreMap.changeFreeMap(freemap).map((_, sdir))
        }

      val maybeBucket = CoreMap.changeFreeMap(bucket)

      case class KO(keys: List[(Data, SortDir)], main: SortDir)

      object KO {
        @SuppressWarnings(Array("org.wartremover.warts.Recursion"))
        implicit val KOOrdering: SOrdering[KO] = new SOrdering[KO] {
          override def compare(ko1: KO, ko2: KO): Int = (ko1, ko2) match {
            case (KO((d1, sd1) :: tail1, _), KO((d2, sd2) :: tail2, _)) if ord.compare(d1, d2) === 0 =>
              compare(ko1.copy(keys = tail1), ko2.copy(keys = tail2))
            case (KO((d1, sd1) :: tail1, main), KO((d2, sd2) :: tail2, _)) if sd1 === main =>
              ord.compare(d1, d2)
            case (KO((d1, _) :: tail1, _), KO((d2, _) :: tail2, _)) =>
              (- ord.compare(d1, d2))
            case (KO(Nil, _), KO(_, _)) => 0
          }
        }
      }

      EitherT((maybeBucket |@| maybeSortBys) {
        case (bucket, sortBys) =>
          val main = sortBys.head._2
          val keys = (bucket, main) :: sortBys.toList
          src.sortBy(d => KO(keys.map(p => p.leftMap(_(d))), main), main === SortDir.Ascending)
      }.point[Task]).liftM[SparkStateT]

    case Filter(src, f) =>
      StateT((sc: SparkContext) =>
        EitherT {
          CoreMap.changeFreeMap(f).map(df => (sc, src.filter(
            df >>> {
              case Data.Bool(b) => b
              case _            => false
            }))).point[Task]
        })
    case Subset(src, from, sel, count) =>
      filterOut(fromFile, src, from, count,
        sel match {
          case Drop => (i: Index, c: Count) => i >= c
          case Take => (i: Index, c: Count) => i < c
          // TODO: Better sampling
          case Sample => (i: Index, c: Count) => i < c
        })

    case LeftShift(src, struct, id, repair) =>

      val structFunc: PlannerError \/ (Data => Data) =
        CoreMap.changeFreeMap(struct)

      def repairFunc: PlannerError \/ ((Data, Data) => Data) =
        CoreMap.changeJoinFunc(repair)

      StateT((sc: SparkContext) =>
        EitherT((structFunc ⊛ repairFunc)((df, rf) =>
          src.flatMap((input: Data) => df(input) match {
            case Data.Arr(list) => id match {
              case ExcludeId => list.map(rf(input, _))
              case IncludeId => list.zipWithIndex.map(p => rf(input, Data.Arr(List(Data.Int(p._2), p._1))))
              case IdOnly => list.indices.map(i => rf(input, Data.Int(i)))
            }
            case Data.Obj(m) => id match {
              case ExcludeId => m.values.map(rf(input, _))
              case IncludeId => m.map(p => Data.Arr(List(Data.Str(p._1), p._2)))
              case IdOnly => m.keys.map(k => rf(input, Data.Str(k)))
            }
            case _ => List.empty[Data]
          })).map((sc, _)).point[Task]))

    case Union(src, lBranch, rBranch) =>
      val algebraM = Planner[QScriptTotal[T, ?]].plan(fromFile)
      val srcState = src.point[SparkState]

      (lBranch.cataM(interpretM(κ(srcState), algebraM)) ⊛
        rBranch.cataM(interpretM(κ(srcState), algebraM)))(_ ++ _)
    case Unreferenced() =>
      StateT((sc: SparkContext) => {
        EitherT((sc, sc.parallelize(List(Data.Null: Data))).right[PlannerError].point[Task])
      })
  }
}

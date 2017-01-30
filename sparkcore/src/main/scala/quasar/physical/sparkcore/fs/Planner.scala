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

package quasar.physical.sparkcore.fs

import quasar.Predef._
import quasar._, quasar.Planner._
import quasar.common.SortDir
import quasar.contrib.pathy.AFile
import quasar.fp.ski._
import quasar.qscript._
import quasar.contrib.pathy.AFile
import quasar.qscript.ReduceFuncs._

import scala.math.{Ordering => SOrdering}
import SOrdering.Implicits._

import org.apache.spark._
import org.apache.spark.rdd._
import matryoshka.{Hole => _, _}
import matryoshka.data._
import matryoshka.implicits._
import matryoshka.patterns._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

trait Planner[F[_]] extends Serializable {
  def plan(fromFile: (SparkContext, AFile) => Task[RDD[String]]): AlgebraM[Planner.SparkState, F, RDD[Data]]
}

// TODO divide planner instances into separate files
object Planner {

  def apply[F[_]](implicit P: Planner[F]): Planner[F] = P

  // TODO consider moving to data.scala (conflicts with existing code)
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

  type SparkState[A] = StateT[EitherT[Task, PlannerError, ?], SparkContext, A]
  type SparkStateT[F[_], A] = StateT[F, SparkContext, A]

  private def unreachable[F[_]](what: String): Planner[F] =
    new Planner[F] {
      def plan(fromFile: (SparkContext, AFile) => Task[RDD[String]]): AlgebraM[SparkState, F, RDD[Data]] =
        _ =>  StateT((sc: SparkContext) => {
        EitherT(InternalError.fromMsg(s"unreachable $what").left[(SparkContext, RDD[Data])].point[Task])
      })
    }

  implicit def deadEnd: Planner[Const[DeadEnd, ?]] = unreachable("deadEnd")
  implicit def read: Planner[Const[Read, ?]] = unreachable("read")
  implicit def projectBucket[T[_[_]]]: Planner[ProjectBucket[T, ?]] = unreachable("projectBucket")
  implicit def thetaJoin[T[_[_]]]: Planner[ThetaJoin[T, ?]] = unreachable("thetajoin")

  implicit def shiftedread: Planner[Const[ShiftedRead, ?]] =
    new Planner[Const[ShiftedRead, ?]] {
      
      def plan(fromFile: (SparkContext, AFile) => Task[RDD[String]]) =
        (qs: Const[ShiftedRead, RDD[Data]]) => {
          StateT((sc: SparkContext) => {
            val filePath = qs.getConst.path
            val idStatus = qs.getConst.idStatus

            EitherT(fromFile(sc, filePath).map { initRDD =>
              val rdd = initRDD.map { raw =>
                DataCodec.parse(raw)(DataCodec.Precise).fold(error => Data.NA, ι)
              }
              (sc,
                idStatus match {
                  case IdOnly => rdd.zipWithIndex.map[Data](p => Data.Int(p._2))
                  case IncludeId =>
                    rdd.zipWithIndex.map[Data](p =>
                      Data.Arr(List(Data.Int(p._2), p._1)))
                  case ExcludeId => rdd
                }).right[PlannerError]
            })
          })
        }
    }

  implicit def qscriptCore[T[_[_]]: RecursiveT: ShowT]:
      Planner[QScriptCore[T, ?]] =
    new Planner[QScriptCore[T, ?]] {
      

      type Index = Long
      type Count = Long

      private def filterOut(
        fromFile: (SparkContext, AFile) => Task[RDD[String]],
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
          case (Data.Str(a), Data.Str(b)) => Data.Str(a) // TODO fix this
          case _ => Data.NA
        }
        case Max(_) => (d1: Data, d2: Data) => (d1, d2) match {
          case (Data.Int(a), Data.Int(b)) => Data.Int(a.max(b))
          case (Data.Number(a), Data.Number(b)) => Data.Dec(a.max(b))
          case (Data.Str(a), Data.Str(b)) => Data.Str(a) // TODO fix this
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

      def plan(fromFile: (SparkContext, AFile) => Task[RDD[String]]): AlgebraM[SparkState, QScriptCore[T, ?], RDD[Data]] = {
        case qscript.Map(src, f) =>
          StateT((sc: SparkContext) =>
            EitherT {
              val maybeFunc =
                // TODO extract to a single method for compile-time efficiency
                f.cataM(interpretM(κ(ι[Data].right[PlannerError]), CoreMap.change))
              maybeFunc.map(df => (sc, src.map(df))).point[Task]
            }
          )
        case Reduce(src, bucket, reducers, repair) =>
          val maybePartitioner: PlannerError \/ (Data => Data) =
            bucket.cataM(interpretM(κ(ι[Data].right[PlannerError]), CoreMap.change))

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
              case Data.Dec(v) => Data.Arr(List(Data.Dec(v), Data.Int(1)))
              case _ => Data.NA
            }
            case Arbitrary(a) => a
            case First(a) => a
            case Last(a) => a
            case UnshiftArray(a) => a >>> ((d: Data) => Data.Arr(List(d)))
            case UnshiftMap(a1, a2) => ((d: Data) => a1(d) match {
              case Data.Str(k) => Data.Obj(ListMap(k -> a2(d)))
              case _ => Data.NA
            })
          }

          val maybeTransformers: PlannerError \/ List[Data => Data] =
            reducers.traverse(red => (red.traverse(_.cataM(interpretM(κ(ι[Data].right[PlannerError]), CoreMap.change)))).map(extractFunc))

          val reducersFuncs: List[(Data,Data) => Data] =
            reducers.map(reduceData)

          val maybeRepair: PlannerError \/ (Data => Data) =
            repair.cataM(interpretM({
              case ReduceIndex(i) => ((x: Data) => x match {
                case Data.Arr(elems) => elems(i)
                case _ => Data.NA
              }).right
            }, CoreMap.change))

          def merge(a: Data, b: Data, f: List[(Data, Data) => Data]): Data = (a, b) match {
            case (Data.Arr(l1), Data.Arr(l2)) => Data.Arr(Zip[List].zipWith(f,(l1.zip(l2))) {
              case (f, (l1, l2)) => f(l1,l2)
            })
            case _ => Data.NA
          }

          StateT((sc: SparkContext) =>
            EitherT(((maybePartitioner |@| maybeTransformers |@| maybeRepair) {
              case (partitioner, trans, repair) =>
                src.map(d => (partitioner(d), Data.Arr(trans.map(_(d))) : Data))
                  .reduceByKey(merge(_,_, reducersFuncs))
                  .map {
                  case (k, Data.Arr(vs)) =>
                    val v = Zip[List].zipWith(vs, reducers) {
                      case (Data.Arr(List(Data.Dec(sum), Data.Int(count))), Avg(_)) => Data.Dec(sum / BigDecimal(count))
                      case (d, _) => d
                    }
                    repair(Data.Arr(v))
                  case (_, _) => Data.NA
                }
            }).map((sc, _)).point[Task])
          )
        case Sort(src, bucket, orders) =>

          val maybeSortBys: PlannerError \/ NonEmptyList[(Data => Data, SortDir)] =
            orders.traverse {
              case (freemap, sdir) =>
                freemap.cataM(interpretM(κ(ι[Data].right[PlannerError]), CoreMap.change)).map((_, sdir))
            }

          val maybeBucket =
            bucket.cataM(interpretM(κ(ι[Data].right[PlannerError]), CoreMap.change))

          EitherT((maybeBucket |@| maybeSortBys) {
            case (bucket, sortBys) =>
              val asc  = sortBys.head._2 === SortDir.Ascending
              val keys = bucket :: sortBys.map(_._1).toList
              src.sortBy(d => keys.map(_(d)), asc)
          }.point[Task]).liftM[SparkStateT]

        case Filter(src, f) =>
          StateT((sc: SparkContext) =>
            EitherT {
              val maybeFunc =
                f.cataM(interpretM(κ(ι[Data].right[PlannerError]), CoreMap.change))
              maybeFunc.map(df => (sc, src.filter{
                df >>> {
                  case Data.Bool(b) => b
                  case _ => false
                }
              })).point[Task]
            }
          )
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
            struct.cataM(interpretM(κ(ι[Data].right[PlannerError]), CoreMap.change))

          def repairFunc: PlannerError \/ ((Data, Data) => Data) = {
            val dd: PlannerError \/ (Data => Data) =
              repair.cataM(interpretM[PlannerError \/ ?, MapFunc[T, ?], JoinSide, Data => Data]({
                case LeftSide => ((x: Data) => x match {
                  case Data.Arr(elems) => elems(0)
                  case _ => Data.NA
                }).right
                case RightSide => ((x: Data) => x match {
                  case Data.Arr(elems) => elems(1)
                  case _ => Data.NA
                }).right
              }, CoreMap.change))

            dd.map(df => (l, r) => df(Data.Arr(List(l, r))))
          }

          StateT((sc: SparkContext) =>
            EitherT((structFunc ⊛ repairFunc)((df, rf) =>
              src.flatMap((input: Data) => (df(input), id) match {
                case (Data.Arr(list), ExcludeId) => list.map(rf(input, _))
                case (Data.Arr(list), IncludeId) =>
                  list.zipWithIndex.map(p => rf(input, Data.Arr(List(Data.Int(p._2), p._1))))
                case (Data.Arr(list), IdOnly) =>
                  list.indices.map(i => rf(input, Data.Int(i)))
                case (Data.Obj(m), ExcludeId) => m.values.map(rf(input, _))
                case (Data.Obj(m), IncludeId) =>
                  m.map(p => Data.Arr(List(Data.Str(p._1), p._2)))
                case (Data.Obj(m), IdOnly) =>
                  m.keys.map(k => rf(input, Data.Str(k)))
                case (_, _) => List.empty[Data]
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

  implicit def equiJoin[T[_[_]]: RecursiveT: ShowT]:
      Planner[EquiJoin[T, ?]] =
    new Planner[EquiJoin[T, ?]] {
      
      def plan(fromFile: (SparkContext, AFile) => Task[RDD[String]]): AlgebraM[SparkState, EquiJoin[T, ?], RDD[Data]] = {
        case EquiJoin(src, lBranch, rBranch, lKey, rKey, jt, combine) =>
          val algebraM = Planner[QScriptTotal[T, ?]].plan(fromFile)
          val srcState = src.point[SparkState]

          def genKey(kf: FreeMap[T]): SparkState[(Data => Data)] = EitherT(kf.cataM(interpretM(κ(ι[Data].right[PlannerError]), CoreMap.change)).point[Task]).liftM[SparkStateT]

          val merger: SparkState[Data => Data] = 
            EitherT((combine.cataM(interpretM[PlannerError \/ ?, MapFunc[T, ?], JoinSide, Data => Data]({
              case LeftSide => ((x: Data) => x match {
                case Data.Arr(elems) => elems(0)
                case _ => Data.NA
              }).right
              case RightSide => ((x: Data) => x match {
                case Data.Arr(elems) => elems(1)
                case _ => Data.NA
              }).right
            }, CoreMap.change))).point[Task]).liftM[SparkStateT]

          for {
            lk <- genKey(lKey)
            rk <- genKey(rKey)
            lRdd <- lBranch.cataM(interpretM(κ(srcState), algebraM))
            rRdd <- rBranch.cataM(interpretM(κ(srcState), algebraM))
            merge <- merger
          } yield {
            val klRdd = lRdd.map(d => (lk(d), d))
            val krRdd = rRdd.map(d => (rk(d), d))

            jt match {
              case Inner => klRdd.join(krRdd).map {
                case (_, (l, r)) => merge(Data.Arr(List(l, r)))
              }
              case LeftOuter => klRdd.leftOuterJoin(krRdd).map {
                case (_, (l, r)) => merge(Data.Arr(List(l, r.getOrElse(Data.Null))))
              }
              case RightOuter => klRdd.rightOuterJoin(krRdd).map {
                case (_, (l, r)) => merge(Data.Arr(List(l.getOrElse(Data.Null), r)))
              }
              case FullOuter => klRdd.fullOuterJoin(krRdd).map {
                case (_, (l, r)) => merge(Data.Arr(List(l.getOrElse(Data.Null), r.getOrElse(Data.Null))))
              }
            }
          }
      }
    }
  
  implicit def coproduct[F[_], G[_]](
    implicit F: Planner[F], G: Planner[G]):
      Planner[Coproduct[F, G, ?]] =
    new Planner[Coproduct[F, G, ?]] {
      
      def plan(fromFile: (SparkContext, AFile) => Task[RDD[String]]): AlgebraM[SparkState, Coproduct[F, G, ?], RDD[Data]] = _.run.fold(F.plan(fromFile), G.plan(fromFile))
    }
}

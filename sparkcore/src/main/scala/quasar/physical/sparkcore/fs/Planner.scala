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
import quasar.contrib.matryoshka._
import quasar.contrib.pathy.AFile
import quasar.fp.MonadError_
import quasar.fp.ski._
import quasar.qscript._
import quasar.contrib.pathy.AFile
import quasar.qscript.ReduceFuncs._

import scala.math.{Ordering => SOrdering}
import SOrdering.Implicits._

import org.apache.spark._
import org.apache.spark.rdd._
import matryoshka.{Hole => _, _}
import scalaz._, Scalaz._

trait Planner[F[_], M[_]] extends Serializable {
  def plan(fromFile: AFile => M[RDD[String]]): AlgebraM[M, F, RDD[Data]]
}

// TODO divide planner instances into separate files
object Planner {

  def apply[F[_], M[_]](implicit P: Planner[F, M]): Planner[F, M] = P

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

  private def unreachable[F[_], M[_]](what: String)(implicit
    merr: MonadError_[M, PlannerError]
  ): Planner[F, M] =
    new Planner[F, M] {
      def plan(fromFile: AFile => M[RDD[String]]): AlgebraM[M, F, RDD[Data]] =
        _ => merr.raiseError(InternalError.fromMsg(s"unreachable $what"))
    }

  implicit def deadEnd[M[_]](implicit
    merr: MonadError_[M, PlannerError]
  ): Planner[Const[DeadEnd, ?], M] = unreachable[Const[DeadEnd, ?], M]("deadEnd")
  implicit def read[M[_]](implicit
    merr: MonadError_[M, PlannerError]
  ): Planner[Const[Read, ?], M] = unreachable[Const[Read, ?], M]("read")
  implicit def projectBucket[T[_[_]], M[_]](implicit
    merr: MonadError_[M, PlannerError]
  ): Planner[ProjectBucket[T, ?], M] = unreachable[ProjectBucket[T, ?], M]("projectBucket")
  implicit def thetaJoin[T[_[_]], M[_]](implicit
    merr: MonadError_[M, PlannerError]
  ): Planner[ThetaJoin[T, ?], M] = unreachable[ThetaJoin[T, ?], M]("thetajoin")

  implicit def shiftedread[M[_]](implicit
    mstate: MonadState[M, SparkContext]
  ): Planner[Const[ShiftedRead, ?], M] =
    new Planner[Const[ShiftedRead, ?], M] {
      
      def plan(fromFile: AFile => M[RDD[String]]) =
        (qs: Const[ShiftedRead, RDD[Data]]) => {

          val filePath = qs.getConst.path
          val idStatus = qs.getConst.idStatus
          for {
            sc <- mstate.get
            initRDD <- fromFile(filePath)
          } yield {
            val rdd = initRDD.map { raw =>
              DataCodec.parse(raw)(DataCodec.Precise).fold(error => Data.NA, ι)
            }
            idStatus match {
              case IdOnly => rdd.zipWithIndex.map[Data](p => Data.Int(p._2))
              case IncludeId =>
                rdd.zipWithIndex.map[Data](p =>
                  Data.Arr(List(Data.Int(p._2), p._1)))
              case ExcludeId => rdd
            }
          }
        }
    }

  implicit def qscriptCore[T[_[_]]: Recursive: ShowT, M[_]](implicit
    merr: MonadError_[M, PlannerError],
    mstate: MonadState[M, SparkContext]
  ): Planner[QScriptCore[T, ?], M] =
    new Planner[QScriptCore[T, ?], M] {

      type Index = Long
      type Count = Long


      private def filterOut(
        fromFile: AFile => M[RDD[String]],
        src: RDD[Data],
        from: FreeQS[T],
        count: FreeQS[T],
        predicate: (Index, Count) => Boolean ): M[RDD[Data]] = {

        val algebraM = Planner[QScriptTotal[T, ?], M].plan(fromFile)

        val fromState: M[RDD[Data]] =
          freeCataM(from)(interpretM(κ(src.point[M]), algebraM))
        val countState: M[RDD[Data]] =
          freeCataM(count)(interpretM(κ(src.point[M]), algebraM))

        val countEval: M[Long] =
          countState.flatMap(_.first match {
            case Data.Int(v) if v.isValidLong => v.toLong.point[M]
            case Data.Int(v) =>
              merr.raiseError(InternalError.fromMsg(s"Provided Integer $v is not a Long"))
            case a =>
              merr.raiseError(InternalError.fromMsg(s"$a is not a Long number"))
          })

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
        case UnshiftArray(a) => (d1: Data, d2: Data) => (d1, d2) match {
          case (Data.Arr(a), Data.Arr(b)) => Data.Arr(a ++ b)
          case _ => Data.NA
        }
        case UnshiftMap(a1, a2) => (d1: Data, d2: Data) => (d1, d2) match {
          case (Data.Obj(a), Data.Obj(b)) => Data.Obj(a ++ b)
          case _ => Data.NA
        }

      }


      def plan(fromFile: AFile => M[RDD[String]]): AlgebraM[M, QScriptCore[T, ?], RDD[Data]] = {
        case qscript.Map(src, f) =>
          freeCataM(f)(interpretM(κ(ι[Data].right[PlannerError]), CoreMap.change))
            .fold(merr.raiseError(_), df => src.map(df).point[M])

        case Reduce(src, bucket, reducers, repair) =>
          val maybePartitioner: PlannerError \/ (Data => Data) =
            freeCataM(bucket)(interpretM(κ(ι[Data].right[PlannerError]), CoreMap.change))

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
            case UnshiftArray(a) => a >>> ((d: Data) => Data.Arr(List(d)))
            case UnshiftMap(a1, a2) => ((d: Data) => a1(d) match {
              case Data.Str(k) => Data.Obj(ListMap(k -> a2(d)))
              case _ => Data.NA
            })
          }

          val maybeTransformers: PlannerError \/ List[Data => Data] =
            reducers.traverse(red => (red.traverse(freeCataM(_: FreeMap[T])(interpretM(κ(ι[Data].right[PlannerError]), CoreMap.change)))).map(extractFunc))

          val reducersFuncs: List[(Data,Data) => Data] =
            reducers.map(reduceData)

          val maybeRepair: PlannerError \/ (Data => Data) =
            freeCataM(repair)(interpretM({
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

          (maybePartitioner |@| maybeTransformers |@| maybeRepair) {
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
          }.fold(merr.raiseError(_), _.point[M])


        case Sort(src, bucket, orders) =>

          val maybeSortBys: PlannerError \/ NonEmptyList[(Data => Data, SortDir)] =
            orders.traverse {
              case (freemap, sdir) =>
                freeCataM(freemap)(interpretM(κ(ι[Data].right[PlannerError]), CoreMap.change)).map((_, sdir))
            }

          val maybeBucket =
            freeCataM(bucket)(interpretM(κ(ι[Data].right[PlannerError]), CoreMap.change))

          (maybeBucket |@| maybeSortBys) {
            case (bucket, sortBys) =>
              val asc  = sortBys.head._2 === SortDir.Ascending
              val keys = bucket :: sortBys.map(_._1).toList
              src.sortBy(d => keys.map(_(d)), asc)
          }.fold(merr.raiseError(_), _.point[M])

        case Filter(src, f) =>
          val maybeFunc =
            freeCataM(f)(interpretM(κ(ι[Data].right[PlannerError]), CoreMap.change))
          maybeFunc.map(df => src.filter {
            df >>> {
              case Data.Bool(b) => b
              case _ => false
            }
          }).fold(merr.raiseError(_), _.point[M])


        case Subset(src, from, sel, count) =>
          filterOut(fromFile, src, from, count,
            sel match {
              case Drop => (i: Index, c: Count) => i >= c
              case Take => (i: Index, c: Count) => i < c
              case Sample => (i: Index, c: Count) => i < c
            })

        case LeftShift(src, struct, id, repair) =>

          val structFunc: PlannerError \/ (Data => Data) =
            freeCataM(struct)(interpretM(κ(ι[Data].right[PlannerError]), CoreMap.change))

          def repairFunc: PlannerError \/ ((Data, Data) => Data) = {
            val dd: PlannerError \/ (Data => Data) =
              freeCataM(repair)(interpretM[PlannerError \/ ?, MapFunc[T, ?], JoinSide, Data => Data]({
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


            (structFunc ⊛ repairFunc)((df, rf) =>
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
              })).fold(merr.raiseError(_), _.point[M])

        case Union(src, lBranch, rBranch) =>
          val algebraM = Planner[QScriptTotal[T, ?], M].plan(fromFile)

          (freeCataM(lBranch)(interpretM(κ(src.point[M]), algebraM)) ⊛
            freeCataM(rBranch)(interpretM(κ(src.point[M]), algebraM)))(_ ++ _)

        case Unreferenced() =>
          mstate.get.map(_.parallelize(List(Data.Null: Data)))
      }
    }
  
  implicit def equiJoin[T[_[_]]: Recursive: ShowT, M[_] : Monad](implicit
    merr: MonadError_[M, PlannerError],
    mstate: MonadState[M, SparkContext]
  ): Planner[EquiJoin[T, ?], M] =
    new Planner[EquiJoin[T, ?], M] {
      
      def plan(fromFile: AFile => M[RDD[String]]): AlgebraM[M, EquiJoin[T, ?], RDD[Data]] = {
        case EquiJoin(src, lBranch, rBranch, lKey, rKey, jt, combine) =>
          val algebraM = Planner[QScriptTotal[T, ?], M].plan(fromFile)

          def genKey(kf: FreeMap[T]): M[(Data => Data)] =
            freeCataM(kf)(interpretM(
              κ(ι[Data].right[PlannerError]),
              CoreMap.change)
            ).fold(merr.raiseError(_), _.point[M])

          val merger: M[Data => Data] = 
            freeCataM(combine)(interpretM[PlannerError \/ ?, MapFunc[T, ?], JoinSide, Data => Data](
              {
                case LeftSide => ((x: Data) => x match {
                  case Data.Arr(elems) => elems(0)
                  case _ => Data.NA
                }).right
                case RightSide => ((x: Data) => x match {
                  case Data.Arr(elems) => elems(1)
                  case _ => Data.NA
                }).right
              },
              CoreMap.change)
            ).fold(merr.raiseError(_), _.point[M])


          for {
            lk <- genKey(lKey)
            rk <- genKey(rKey)
            lRdd <- freeCataM(lBranch)(interpretM(κ(src.point[M]), algebraM))
            rRdd <- freeCataM(rBranch)(interpretM(κ(src.point[M]), algebraM))
            merge <- merger
          } yield {
            val klRdd = lRdd.map(d => (lk(d), d))
            val krRdd = rRdd.map(d => (rk(d), d))

            jt match {
              case Inner => klRdd.join(krRdd).map {
                case (k, (l, r)) => merge(Data.Arr(List(l, r)))
              }
              case LeftOuter => klRdd.leftOuterJoin(krRdd).map {
                case (k, (l, Some(r))) => merge(Data.Arr(List(l, r)))
                case (k, (l, None)) => merge(Data.Arr(List(l, Data.Null)))
              }
              case RightOuter => klRdd.rightOuterJoin(krRdd).map {
                case (k, (Some(l), r)) => merge(Data.Arr(List(l, r)))
                case (k, (None, r)) => merge(Data.Arr(List(Data.Null, r)))
              }
              case FullOuter => klRdd.fullOuterJoin(krRdd).map {
                case (k, (Some(l), Some(r))) => merge(Data.Arr(List(l, r)))
                case (k, (Some(l), None)) => merge(Data.Arr(List(l, Data.Null)))
                case (k, (None, Some(r))) => merge(Data.Arr(List(Data.Null, r)))
                case (k, (None, None)) => merge(Data.Arr(List(Data.Null, Data.Null)))
              }
            }
          }

      }
    }

  implicit def coproduct[F[_], G[_], M[_]](
    implicit F: Planner[F, M], G: Planner[G, M]):
      Planner[Coproduct[F, G, ?], M] =
    new Planner[Coproduct[F, G, ?], M] {
      
      def plan(fromFile: AFile => M[RDD[String]]): AlgebraM[M, Coproduct[F, G, ?], RDD[Data]] = _.run.fold(F.plan(fromFile), G.plan(fromFile))
    }

}
